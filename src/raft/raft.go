package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"

	"bytes"
	"fmt"
	"log"
	"math/rand"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labgob"
	"6.824/labrpc"
)

const (
	electionTORandBound = 500
)

var (
	electionTO        = time.Duration(1) * time.Second
	heartbeatTimeSpan = time.Duration(100) * time.Millisecond
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type LogEntry struct {
	Term         int
	Command      interface{}
	CommandValid bool
}

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	debug bool

	isLeader        bool
	isLeaderCond    sync.Cond
	electionTimeRef time.Time

	term     int
	votedFor int

	log []LogEntry

	snapshotIdx  int
	snapshotTerm int

	commitIdx      int
	lastAppliedIdx int

	nextIdx  []int
	matchIdx []int

	applyCh chan ApplyMsg
}

func (rf *Raft) offset() int {
	if rf.snapshotIdx == 0 {
		return 0
	}
	return rf.snapshotIdx + 1
}

func (rf *Raft) logIdx() int {
	return rf.offset() + len(rf.log)
}

func (rf *Raft) lastLogIdx() int {
	return rf.logIdx() - 1
}

func (rf *Raft) lastLogTerm() int {
	if len(rf.log) == 0 {
		return rf.snapshotTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) idxTerm(idx int) int {
	if idx-rf.offset() >= 0 {
		return rf.log[idx-rf.offset()].Term
	}
	// A call with idx < snapshotIdx
	// should never happen in practice
	// as implemented in calling funcs
	return rf.snapshotTerm
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.term
	isleader = rf.isLeader

	return term, isleader
}

func (rf *Raft) state() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var err error
	if err = e.Encode(rf.term); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return nil
	}
	if err = e.Encode(rf.votedFor); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return nil
	}
	if err = e.Encode(rf.snapshotIdx); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return nil
	}
	if err = e.Encode(rf.snapshotTerm); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return nil
	}
	if err = e.Encode(rf.log); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return nil
	}

	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	rf.persister.SaveRaftState(rf.state())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 {
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, votedFor, snapshotIdx, snapshotTerm int
	var log []LogEntry

	// In case of error the func signature does not
	// allow us to propagate the error, so just return
	var err error
	if err = d.Decode(&term); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}
	if err = d.Decode(&votedFor); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}
	if err = d.Decode(&snapshotIdx); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}
	if err = d.Decode(&snapshotTerm); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}
	if err = d.Decode(&log); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}

	rf.term = term
	rf.votedFor = votedFor
	rf.snapshotIdx = snapshotIdx
	rf.snapshotTerm = snapshotTerm
	rf.commitIdx = snapshotIdx
	rf.lastAppliedIdx = snapshotIdx
	rf.log = log
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).
	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.lf("received Snapshot RPC req with lastIncludedIdx: %d", index)

	// Do not block service
	go func() {
		rf.mu.Lock()
		defer rf.mu.Unlock()

		if index <= rf.snapshotIdx {
			rf.lf("denying service snapshot. current snapshot is more up to date. %d vs %d",
				index, rf.snapshotIdx)
			return
		}

		var snapshotTerm int

		if index-rf.offset() >= len(rf.log)-1 {
			snapshotTerm = rf.lastLogTerm()
			rf.log = []LogEntry{}
		} else {
			snapshotTerm = rf.idxTerm(index)
			rf.log = rf.log[index-rf.offset()+1:]
		}

		rf.snapshotIdx = index
		rf.snapshotTerm = snapshotTerm

		rf.persister.SaveStateAndSnapshot(rf.state(), snapshot)
	}()
}

type InstallSnapshotArgs struct {
	Term             int
	LeaderId         int
	LastIncludedIdx  int
	LastIncludedTerm int
	Data             []byte
}

type InstallSnapshotReply struct {
	Term int
}

// InstallSnapshot is the Install Snapshot RPC handler implementation for a Raft peer.
func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.lf("received InstallSnapshot RPC req with lastIncludedIdx: %d lastIncludedTerm: %d",
		args.LastIncludedIdx, args.LastIncludedTerm)

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	if args.Term < rf.term {
		rf.lf("denying leader snapshot. current snapshot term is more up to date. %d vs %d",
			args.Term, rf.term)
		reply.Term = rf.term
		return
	}

	if args.Term > rf.term {
		rf.lf("received higher term %d vs %d. updating status", args.Term, rf.term)
		rf.term = args.Term
		rf.votedFor = -1
	}

	rf.isLeader = false
	rf.electionTimeRef = time.Now()

	if args.LastIncludedIdx <= rf.snapshotIdx ||
		args.LastIncludedIdx <= rf.lastAppliedIdx {
		rf.lf("denying leader snapshot. peer status is more up to date")
		return
	}

	if args.LastIncludedIdx-rf.offset() < len(rf.log) &&
		rf.idxTerm(args.LastIncludedIdx) == args.LastIncludedTerm {
		rf.log = rf.log[args.LastIncludedIdx-rf.offset()+1:]
	} else {
		rf.log = []LogEntry{}
	}

	rf.snapshotIdx = args.LastIncludedIdx
	rf.snapshotTerm = args.LastIncludedTerm

	if rf.snapshotIdx > rf.commitIdx {
		rf.commitIdx = rf.snapshotIdx
		rf.lastAppliedIdx = rf.snapshotIdx
	}

	rf.persister.SaveStateAndSnapshot(rf.state(), args.Data)

	rf.applyCh <- ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIdx,
	}
}

func (rf *Raft) sendInstallSnapshot(peer int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[peer].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term        int
	CandidateID int
	LastLogIdx  int
	LastLogTerm int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.l("received RequestVote RPC req")

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	// Your code here (2A, 2B).
	if args.Term < rf.term {
		rf.lf("denying vote to peer %d due to term mismatch", args.CandidateID)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	if args.Term > rf.term {
		rf.term = args.Term
		rf.isLeader = false
		rf.votedFor = -1
	}

	reply.Term = rf.term

	// Grant vote if candidate log is at least as up to date as peer.
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	canVote := rf.votedFor == -1 || rf.votedFor == args.CandidateID
	logUptoDate := args.LastLogTerm > rf.lastLogTerm() ||
		(args.LastLogTerm == rf.lastLogTerm() && args.LastLogIdx >= rf.lastLogIdx())

	if canVote && logUptoDate {
		rf.lf("granting vote to peer %d", args.CandidateID)
		reply.VoteGranted = true

		rf.electionTimeRef = time.Now()
		rf.votedFor = args.CandidateID
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type AppendEntriesArgs struct {
	Term            int
	LeaderID        int
	PrevLogIdx      int
	PrevLogTerm     int
	LeaderCommitIdx int
	Entries         []LogEntry
}

type AppendEntriesReply struct {
	Term         int
	Success      bool
	ConflictIdx  int
	ConflictTerm int
}

// AppendEntries is the Append Entries RPC handler implementation for a Raft peer.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.l("received AppendEntries RPC req")

	rf.mu.Lock()
	defer rf.mu.Unlock()
	defer rf.persist()

	reply.Term = rf.term
	reply.Success = false
	reply.ConflictTerm = -1

	if args.Term < rf.term {
		rf.lf("received lower term %d vs %d. returning false", args.Term, rf.term)
		return
	}

	if args.Term > rf.term {
		rf.lf("received higher term %d vs %d. updating status", args.Term, rf.term)
		rf.term = args.Term
		reply.Term = rf.term
		rf.votedFor = -1
	}

	// Reset self election TO
	rf.isLeader = false
	rf.electionTimeRef = time.Now()

	if len(args.Entries) == 0 {
		rf.lf("received heartbeat with no entries")
	} else {
		rf.lf("received %d entries", len(args.Entries))
	}

	if args.PrevLogIdx < rf.snapshotIdx {
		reply.ConflictIdx = rf.snapshotIdx + 1
		return
	}

	// Reply false if log doesn’t contain entry at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIdx > rf.lastLogIdx() {
		rf.lf("received mismatched entry at idx: %d. peer log does not contain entry",
			args.PrevLogIdx)
		reply.ConflictIdx = rf.lastLogIdx() + 1
		return
	}
	if rf.idxTerm(args.PrevLogIdx) != args.PrevLogTerm {
		reply.ConflictTerm = rf.idxTerm(args.PrevLogIdx)
		for i := args.PrevLogIdx; i > rf.snapshotIdx && rf.idxTerm(i) == reply.ConflictTerm; i-- {
			reply.ConflictIdx = i
		}
		rf.lf("received mismatched entry at idx: %d. conflict_term: %d conflict_idx: %d",
			args.PrevLogIdx, reply.ConflictTerm, reply.ConflictIdx)
		return
	}

	// If an existing entry conflicts with a new one (same index but
	// different terms), delete the existing entry and all that follow it
	j := 0
	i := args.PrevLogIdx + 1
	for i < rf.lastLogIdx()+1 && j < len(args.Entries) {
		if rf.idxTerm(i) != args.Entries[j].Term {
			rf.lf("conflictive entry at idx %d. peer_term: %d leader_term: %d. truncating log until idx; %d",
				i, rf.idxTerm(i), args.Entries[j].Term, i)
			rf.log = rf.log[:i-rf.offset()]
			break
		}
		i++
		j++
	}

	// Append any new entries not already in the log
	for ; j < len(args.Entries); j++ {
		rf.lf("appending new entry %v at idx: %d", args.Entries[j].Command, rf.logIdx())
		rf.log = append(rf.log, args.Entries[j])
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommitIdx > rf.commitIdx {
		lastNewEntryIdx := rf.lastLogIdx()
		if args.LeaderCommitIdx < lastNewEntryIdx {
			rf.commitIdx = args.LeaderCommitIdx
		} else {
			rf.commitIdx = lastNewEntryIdx
		}

		for i := rf.lastAppliedIdx + 1; i <= rf.commitIdx; i++ {
			rf.lf("applying cmd %v at idx: %d to state machine", rf.log[i-rf.offset()].Command, i)
			rf.applyCh <- ApplyMsg{
				CommandValid: rf.log[i-rf.offset()].CommandValid,
				Command:      rf.log[i-rf.offset()].Command,
				CommandIndex: i,
			}
		}
		rf.lastAppliedIdx = rf.commitIdx
	}

	reply.Success = true
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader {
		return index, term, rf.isLeader // false
	}

	index = rf.logIdx()
	term = rf.term
	isLeader = rf.isLeader

	rf.log = append(rf.log, LogEntry{
		Term:         rf.term,
		Command:      command,
		CommandValid: true,
	})
	go rf.broadcastAppendEntries()
	rf.persist()

	rf.lf("start command received with value %v. logIdx is now: %d", command, rf.logIdx())

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.isLeader = false
	rf.l("killed!")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().

		rf.mu.Lock()
		if !rf.isLeader {
			elapsedTime := time.Since(rf.electionTimeRef).Milliseconds()
			if elapsedTime > electionTO.Milliseconds() {
				go rf.startElection(rf.electionTimeRef)
			}
		}
		rf.mu.Unlock()

		randTime := time.Duration(rand.Intn(electionTORandBound))
		time.Sleep(electionTO + time.Duration(randTime)*time.Millisecond)
	}
}

func (rf *Raft) startElection(timeRef time.Time) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	rf.l("starting election")

	// Because we lost the lock temporarily, verify
	// that the time reference due to which the
	// election was started is still the same
	if !rf.electionTimeRef.Equal(timeRef) {
		return
	}

	me := rf.me
	rf.term++
	rf.votedFor = rf.me
	rf.persist()

	args := &RequestVoteArgs{
		Term:        rf.term,
		CandidateID: rf.me,
		LastLogIdx:  rf.lastLogIdx(),
		LastLogTerm: rf.lastLogTerm(),
	}

	votes := 1 // self vote
	majority := len(rf.peers)/2 + 1

	for i := range rf.peers {
		if i != me {
			rf.lf("sending vote request for peer %d", i)
			go func(peer int) {
				reply := &RequestVoteReply{}
				if ok := rf.sendRequestVote(peer, args, reply); !ok {
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.lf("received RequestVote RPC reply from peer: %d", peer)

				// If peer term has changed during RPC
				// calls then abort reading replies
				if rf.term != args.Term {
					return
				}

				if reply.VoteGranted {
					rf.lf("received vote from peer: %d", peer)
					votes++
					if votes >= majority {
						rf.isLeader = true
						rf.isLeaderCond.Signal() // awake leader goroutine
						return
					}
					// We could continue here and assume that
					// term is correct due to vote being granted
					// continue
				}

				if reply.Term > rf.term {
					rf.term = reply.Term
					rf.isLeader = false
					rf.votedFor = -1
					rf.persist()
					return
				}
			}(i)
		}
	}
}

func (rf *Raft) startLeaderLoop() {
	rf.l("starting leader loop")

	var reset bool

	for rf.killed() == false {
		rf.mu.Lock()

		if rf.isLeader {
			// Reset volatile state if just
			// been reelected as leader
			if reset {
				for i := range rf.nextIdx {
					rf.nextIdx[i] = rf.lastLogIdx() + 1
					rf.matchIdx[i] = 0
				}
				reset = false
			}

			rf.mu.Unlock()
			rf.broadcastAppendEntries()
			time.Sleep(heartbeatTimeSpan)
		} else {
			rf.mu.Unlock()

			// Wait to be signalled as leader
			rf.l("waiting to be signalled leader")
			rf.isLeaderCond.L.Lock()
			rf.isLeaderCond.Wait()
			rf.isLeaderCond.L.Unlock()
			rf.l("is leader")
			reset = true
		}
	}
}

func (rf *Raft) broadcastAppendEntries() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader {
		return
	}

	rf.l("broadcasting AppendEntries as leader")

	for i := range rf.peers {
		if i != rf.me {
			peerNextIdx := rf.nextIdx[i]

			// If peer's log is behind end of last snapshot,
			// send complete snapshot instead of entries
			if peerNextIdx <= rf.snapshotIdx {
				rf.lf(
					"sending InstallSnapshot RPC to peer %d. term: %d snapshotIdx: %d snapshotTerm: %d",
					i, rf.term, rf.snapshotIdx, rf.snapshotTerm,
				)
				go rf.requestInstallSnapshot(i, InstallSnapshotArgs{
					Term:             rf.term,
					LeaderId:         rf.me,
					LastIncludedIdx:  rf.snapshotIdx,
					LastIncludedTerm: rf.snapshotTerm,
					Data:             rf.persister.ReadSnapshot(),
				})
				continue
			}

			args := &AppendEntriesArgs{
				Term:            rf.term,
				LeaderID:        rf.me,
				PrevLogIdx:      peerNextIdx - 1,
				PrevLogTerm:     rf.idxTerm(peerNextIdx - 1),
				LeaderCommitIdx: rf.commitIdx,
			}
			entries := rf.log[peerNextIdx-rf.offset():]
			args.Entries = make([]LogEntry, len(entries))
			copy(args.Entries, entries)

			rf.lf(
				"sending AppendEntries RPC to peer %d. term: %d prevTerm: %d prevIdx: %d entries: %d",
				i, rf.term, args.PrevLogTerm, args.PrevLogIdx, len(args.Entries),
			)

			go func(peer, logIdx int) {
				reply := &AppendEntriesReply{}
				if ok := rf.sendAppendEntries(peer, args, reply); !ok {
					rf.lf("leader can not reach peer %d", peer)
					return
				}

				rf.mu.Lock()
				defer rf.mu.Unlock()
				rf.lf("received AppendEntries RPC reply. peer: %d term: %d success: %v", peer, reply.Term, reply.Success)

				if !rf.isLeader {
					return
				}

				if reply.Success {
					rf.nextIdx[peer] = logIdx
					rf.matchIdx[peer] = logIdx - 1
				} else {
					if reply.Term > rf.term {
						// Become follower
						rf.term = reply.Term
						rf.isLeader = false
						rf.votedFor = -1
						rf.persist()
						return
					} else if reply.ConflictTerm < 0 {
						// Sent entries are previous to follower's snapshot or
						// sent entries are posterior to follower's log so there
						// are missing entries in between
						rf.nextIdx[peer] = min(reply.ConflictIdx, rf.logIdx())
						rf.matchIdx[peer] = rf.nextIdx[peer] - 1
					} else {
						// Try to find conflict term in leader log
						nextIdx := rf.lastLogIdx()
						for ; nextIdx > rf.snapshotIdx; nextIdx-- {
							if rf.idxTerm(nextIdx) == reply.ConflictTerm {
								break
							}
						}
						// If not found, set peer nextIdx to conflictIdx
						if nextIdx == rf.snapshotIdx {
							rf.nextIdx[peer] = reply.ConflictIdx
						} else {
							rf.nextIdx[peer] = nextIdx
						}
						rf.matchIdx[peer] = rf.nextIdx[peer] - 1
					}
				}

				rf.applyMssgs()
			}(i, rf.logIdx()) // Copy logIdx reference for entries sent to followers
		}
	}
}

func (rf *Raft) requestInstallSnapshot(peer int, args InstallSnapshotArgs) {
	reply := InstallSnapshotReply{}
	if ok := rf.sendInstallSnapshot(peer, &args, &reply); !ok {
		rf.lf("leader can not reach peer %d", peer)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.isLeader || rf.term != args.Term {
		return
	}

	if reply.Term > rf.term {
		rf.term = reply.Term
		rf.isLeader = false
		rf.votedFor = -1
		rf.persist()
		return
	}

	rf.matchIdx[peer] = args.LastIncludedIdx
	rf.nextIdx[peer] = rf.matchIdx[peer] + 1
}

func (rf *Raft) applyMssgs() {
	// If maximum replicated index across most followers is higher than current
	// leader commitIdx, then apply the "in between" entries to state machine
	if mxRIdx := rf.maxReplicatedIdx(); mxRIdx > rf.lastAppliedIdx {
		rf.lf("leader apply mssgs: applying entries from: %d to: %d", rf.lastAppliedIdx+1, mxRIdx)
		for i := rf.lastAppliedIdx + 1; i <= mxRIdx; i++ {
			rf.lf("leader apply mssgs: applying cmd: %v at idx: %d", rf.log[i-rf.offset()].Command, i)
			rf.applyCh <- ApplyMsg{
				CommandValid: rf.log[i-rf.offset()].CommandValid,
				Command:      rf.log[i-rf.offset()].Command,
				CommandIndex: i,
			}
		}
		rf.commitIdx = mxRIdx
		rf.lastAppliedIdx = mxRIdx
	} else {
		rf.lf("leader apply mssgs: no actions. maxReplicatedIdx: %d rf.commitIdx: %d", mxRIdx, rf.commitIdx)
	}
}

// maxReplicatedIdx returns the highest log index for which the
// majority of peers have already replicated. Returns -1 if no
// index complies with these conditions.
func (rf *Raft) maxReplicatedIdx() int {
	maxReplicatedIdx := -1
	for i := rf.lastLogIdx(); i > rf.commitIdx; i-- {
		nReplicated := 1
		if rf.idxTerm(i) == rf.term {
			for p := 0; p < len(rf.peers); p++ {
				if p != rf.me && rf.matchIdx[p] >= i {
					nReplicated++
				}
			}
		}
		if nReplicated > len(rf.peers)/2 {
			maxReplicatedIdx = i
			break
		}
	}
	return maxReplicatedIdx
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).
	if strings.EqualFold(os.Getenv("RAFT_DBG"), "true") {
		rf.debug = true
	}

	rf.votedFor = -1
	rf.isLeaderCond = *sync.NewCond(&sync.Mutex{})
	rf.electionTimeRef = time.Now()
	rf.applyCh = applyCh
	rf.log = append(rf.log, LogEntry{Term: 0})

	rf.nextIdx = make([]int, len(peers))
	for i := range peers {
		rf.nextIdx[i] = rf.lastLogIdx() + 1
	}
	rf.matchIdx = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startLeaderLoop()

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}

// l logs the given string s for the rf peer.
func (rf *Raft) l(s string) {
	if rf.debug {
		defColor := "\033[0m"
		color := fmt.Sprintf("\033[%vm", 31+rf.me)
		prefix := fmt.Sprintf("%s[PEER %d] ", color, rf.me)
		log.Print(prefix, s, defColor)
	}
}

// lf logs the given string format with fields values for the rf peer.
func (rf *Raft) lf(format string, fields ...interface{}) {
	rf.l(fmt.Sprintf(format, fields...))
}

func min(x, y int) int {
	if x < y {
		return x
	}
	return y
}
