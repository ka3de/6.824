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
	electionTORandBound = 500 // TODO: Rename
)

var (
	electionTO        = time.Duration(1) * time.Second
	heartbeatTimeSpan = time.Duration(150) * time.Millisecond
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
	Term    int
	Command interface{}
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

	debug           bool
	isLeader        bool
	isLeaderCond    sync.Cond
	electionTimeRef time.Time // TODO: Best way?

	term     int
	votedFor int

	log    []LogEntry
	logIdx int

	commitIdx      int
	lastAppliedIdx int

	nextIdx  []int
	matchIdx []int

	applyCh chan ApplyMsg
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

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)

	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var err error
	if err = e.Encode(rf.term); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return
	}
	if err = e.Encode(rf.votedFor); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return
	}
	if err = e.Encode(rf.logIdx); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return
	}
	if err = e.Encode(rf.log); err != nil {
		rf.lf("error encoding persistent state: %v", err)
		return
	}

	rf.persister.SaveRaftState(w.Bytes())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	// Your code here (2C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var term, votedFor, logIdx int
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
	if err = d.Decode(&logIdx); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}
	if err = d.Decode(&log); err != nil {
		rf.lf("error decoding persisted state: %v", err)
		return
	}

	// rf.lf("retrieved persistent state: term=%d votedFor=%d logIdx=%d log=%v",
	// 	term, votedFor, logIdx, log)

	rf.term = term
	rf.votedFor = votedFor
	rf.logIdx = logIdx
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

	// Your code here (2A, 2B).
	if args.Term > rf.term {
		rf.term = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	if args.Term < rf.term {
		rf.lf("denying vote to peer %d due to term mismatch", args.CandidateID)
		reply.Term = rf.term
		reply.VoteGranted = false
		return
	}

	// Handle lastLogTerm when log has no entries
	lastLogTerm := rf.term
	if rf.logIdx > 1 {
		lastLogTerm = rf.log[rf.logIdx-1-1].Term // -1 for last one and -1 to handle 1 as starting idx
	}

	// Grant vote if candidate log is at least as up to date as peer.
	// Raft determines which of two logs is more up-to-date
	// by comparing the index and term of the last entries in the
	// logs. If the logs have last entries with different terms, then
	// the log with the later term is more up-to-date. If the logs
	// end with the same term, then whichever log is longer is
	// more up-to-date.
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateID) &&
		(args.LastLogTerm > lastLogTerm ||
			(args.LastLogTerm == lastLogTerm && args.LastLogIdx >= rf.logIdx)) {
		rf.lf("granting vote to peer %d", args.CandidateID)
		reply.Term = rf.term
		reply.VoteGranted = true

		// TODO: Should we reset the lastAppendEntries
		// time ref only when vote has been granted?
		rf.electionTimeRef = time.Now()
		rf.votedFor = args.CandidateID
		rf.persist()
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
	Term    int
	Success bool
}

// AppendEntries is the Append Entries RPC handler implementation for a Raft peer.
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.l("received AppendEntries RPC req")

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// Reset self election TO
	rf.isLeader = false
	rf.electionTimeRef = time.Now()

	if args.Term > rf.term {
		rf.lf("received higher term %d vs %d. updating status", args.Term, rf.term)
		rf.term = args.Term
		rf.votedFor = -1
		rf.persist()
	}

	// Reply false if term < currentTerm
	if args.Term < rf.term {
		rf.lf("received lower term %d vs %d. returning false", args.Term, rf.term)
		reply.Term = rf.term
		reply.Success = false
		return
	}

	if len(args.Entries) == 0 {
		rf.lf("received heartbeat with no entries")
	} else {
		rf.lf("received %d entries", len(args.Entries))
	}

	// Reply false if log doesn’t contain an entry
	// at prevLogIndex whose term matches prevLogTerm
	if args.PrevLogIdx > 0 && (args.PrevLogIdx >= rf.logIdx ||
		rf.log[args.PrevLogIdx-1].Term != args.PrevLogTerm) {
		rf.lf("received mismatched entry at idx: %d", args.PrevLogIdx)
		reply.Term = rf.term
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index
	// but different terms), delete the existing entry and all that
	// follow it
	j := 0
	i := args.PrevLogIdx
	for i < args.PrevLogIdx+len(args.Entries) {
		if i < len(rf.log) {
			currEntry := rf.log[i]
			if currEntry.Term != args.Entries[j].Term {
				// Delete existing conflictive entry and all that follow it
				rf.lf("conflictive entry at idx %d. peer term=%d leader term=%d",
					i, currEntry.Term, args.Entries[j].Term)
				rf.log = rf.log[:i]
				rf.logIdx = i + 1
				continue
			}
		} else {
			// Append any new entries not already in the log
			rf.lf("appending new entry %d at idx: %d", args.Entries[j].Command, rf.logIdx)
			rf.log = append(rf.log, args.Entries[j])
			rf.logIdx++
			rf.persist() // TODO: persist just once after loop?
		}

		i++
		j++
	}

	// If leaderCommit > commitIndex, set commitIndex =
	// min(leaderCommit, index of last new entry)
	if args.LeaderCommitIdx > rf.commitIdx {
		lastNewEntryIdx := rf.logIdx - 1
		if args.LeaderCommitIdx < lastNewEntryIdx {
			rf.commitIdx = args.LeaderCommitIdx
		} else {
			rf.commitIdx = lastNewEntryIdx
		}

		for i := rf.lastAppliedIdx; i < rf.commitIdx; i++ {
			rf.lf("applying cmd %v at idx %d to state machine", rf.log[i].Command, i+1)
			// TODO: Should we avoid possible lock here when sending through channel?
			rf.applyCh <- ApplyMsg{
				CommandValid: true, // TODO: ?
				Command:      rf.log[i].Command,
				CommandIndex: i + 1, // TODO: +1 or not?
				// TODO: 2D
				// SnapshotValid: ,
				// Snapshot: ,
				// SnapshotTerm: ,
				// SnapshotIndex: ,
			}
		}
		rf.lastAppliedIdx = rf.commitIdx // TODO: Increment lastAppliedIdx inside loop!?
	}

	reply.Term = rf.term
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

	index = rf.logIdx
	term = rf.term
	isLeader = rf.isLeader

	rf.log = append(rf.log, LogEntry{
		Term:    rf.term,
		Command: command,
	})
	rf.logIdx++
	rf.persist()

	rf.lf("start command received with value %v. logIdx is now: %d", command, rf.logIdx)

	// TODO: Should we respong to client only
	// when entry has been comitted to state machine?
	// "respond after entry applied to state machine ($5.3)"
	// But this seems to collide with this method's doc

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
	rf.l("starting election")

	rf.mu.Lock()

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

	// Handle lastLogTerm when log has no entries
	lastLogTerm := rf.term
	if rf.logIdx > 1 {
		lastLogTerm = rf.log[rf.logIdx-1-1].Term // -1 for last one and -1 to handle 1 as starting idx
	}

	args := &RequestVoteArgs{
		Term:        rf.term,
		CandidateID: rf.me,
		LastLogIdx:  rf.logIdx,
		LastLogTerm: lastLogTerm,
	}

	votes := 1 // self vote
	majority := len(rf.peers)/2 + 1

	var wg sync.WaitGroup
	replyCh := make(chan *RequestVoteReply, len(rf.peers)-1)

	for i := range rf.peers {
		if i != me {
			rf.lf("sending vote request for peer %d", i)
			wg.Add(1)
			go func(wg *sync.WaitGroup, peer int, replyCh chan<- *RequestVoteReply) {
				reply := &RequestVoteReply{}
				if ok := rf.sendRequestVote(peer, args, reply); ok {
					replyCh <- reply
				}
				wg.Done()
			}(&wg, i, replyCh)
		}
	}

	rf.mu.Unlock()

	go func() {
		wg.Wait()
		close(replyCh)
	}()

	go func() {
		for r := range replyCh {
			rf.l("received RequestVote RPC reply")
			rf.mu.Lock()

			// If peer term has changed during RPC
			// calls then abort reading replies
			if rf.term != args.Term {
				rf.mu.Unlock()
				break
			}

			if r.VoteGranted {
				rf.l("received vote")
				votes++
				if votes >= majority {
					rf.isLeader = true
					rf.isLeaderCond.Signal() // awake leader goroutine
					rf.mu.Unlock()
					break
				}
				// We could continue here and assume that
				// term is correct due to vote being granted
				// continue
			}

			if r.Term > rf.term {
				rf.term = r.Term
				rf.isLeader = false
				rf.votedFor = -1
				rf.persist()
				rf.mu.Unlock()
				break
			}

			rf.mu.Unlock()
		}
	}()
}

func (rf *Raft) startLeaderLoop() {
	rf.l("starting leader loop")

	var reset bool

	for rf.killed() == false {
		rf.mu.Lock()

		if rf.isLeader {
			rf.l("starting AppendEntries loop as leader")

			type peerReply struct {
				peer  int
				reply *AppendEntriesReply
			}

			// Copy current leader logIdx so we get
			// a reference for the entries sent to
			// followers. This way we avoid rf.logIdx
			// from being modified between AppendEntries
			// RPCs and their responses
			logIdx := rf.logIdx

			// Reset volatile state if just
			// been reelected as leader
			if reset {
				for i := range rf.nextIdx {
					rf.nextIdx[i] = rf.logIdx // TODO: Because logIdx points to next void pos? YES! see figure 7
					rf.matchIdx[i] = 0        // TODO: Is this correct? It will be set after first AppendEntries to peer?
				}
				reset = false
			}

			var wg sync.WaitGroup
			var replyCh = make(chan peerReply)

			for i := range rf.peers {
				if i != rf.me {
					peerNextIdx := rf.nextIdx[i]

					var entries []LogEntry
					if rf.logIdx > peerNextIdx {
						entries = rf.log[peerNextIdx-1:]
					}

					// Handle prevLogTerm when log has no entries
					prevLogTerm := rf.term
					if peerNextIdx > 1 {
						prevLogTerm = rf.log[peerNextIdx-1-1].Term // -1 for prev and -1 to handle 1 as starting idx
					}

					args := &AppendEntriesArgs{
						Term:            rf.term,
						LeaderID:        rf.me,
						PrevLogIdx:      peerNextIdx - 1, // TODO: Is this correct?
						PrevLogTerm:     prevLogTerm,
						LeaderCommitIdx: rf.commitIdx,
						Entries:         entries,
					}

					rf.lf(
						"sending AppendEntries RPC to peer %d. term: %d entries: %d",
						i, rf.term, len(entries),
					)

					wg.Add(1)
					go func(wg *sync.WaitGroup, peer int, replyCh chan<- peerReply) {
						reply := &AppendEntriesReply{}
						if ok := rf.sendAppendEntries(peer, args, reply); ok {
							replyCh <- peerReply{peer: peer, reply: reply}
						}
						wg.Done()
					}(&wg, i, replyCh)
				}
			}

			rf.mu.Unlock()

			go func() {
				wg.Wait()
				close(replyCh)
			}()

			go func() {
				for r := range replyCh {
					rf.mu.Lock()
					rf.lf("received AppendEntries RPC reply. term: %d success: %v", r.reply.Term, r.reply.Success)

					if !rf.isLeader {
						rf.mu.Unlock()
						break
					}

					if r.reply.Success {
						rf.nextIdx[r.peer] = logIdx
						rf.matchIdx[r.peer] = logIdx - 1
					} else {
						if r.reply.Term > rf.term {
							// Become follower
							rf.term = r.reply.Term
							rf.isLeader = false
							rf.votedFor = -1
							rf.persist()
							rf.mu.Unlock()
							break
						}
						// Handle unsuccessful reply due to log inconsistency
						rf.nextIdx[r.peer] = rf.nextIdx[r.peer] - 1
					}
					rf.mu.Unlock()
				}
			}()

			time.Sleep(heartbeatTimeSpan)
		} else {
			rf.mu.Unlock()

			// Wait to be signaled as leader
			rf.l("waiting to be signaled leader")
			rf.isLeaderCond.L.Lock()
			rf.isLeaderCond.Wait()
			rf.isLeaderCond.L.Unlock()
			rf.l("is leader")
			reset = true
		}
	}
}

func (rf *Raft) startApplyMssgLoop() {
	rf.l("starting applyMssg loop")

	for rf.killed() == false {
		rf.mu.Lock()

		if rf.isLeader {
			maxReplicatedIdx := maxReplicatedIdx(rf.me, rf.term, len(rf.peers), rf.log, rf.matchIdx)

			rf.lf("leader apply loop: maxReplicatedIdx: %d", maxReplicatedIdx)

			// If maximum replicated index across most followers is
			// higher than current leader commitIdx, then apply the
			// "in between" entries to state machine
			if maxReplicatedIdx > rf.commitIdx {
				rf.lf("leader apply loop: applying entries from: %d to: %d", rf.commitIdx, maxReplicatedIdx)
				for i := rf.commitIdx; i < maxReplicatedIdx; i++ {
					// TODO: Should we do this sending async? Can it be blocked?
					rf.lf("leader apply loop: applying cmd: %v", rf.log[i].Command)
					rf.applyCh <- ApplyMsg{
						CommandValid: true, // TODO: ?
						Command:      rf.log[i].Command,
						CommandIndex: i + 1,
						// TODO: 2D
						// SnapshotValid: ,
						// Snapshot: ,
						// SnapshotTerm: ,
						// SnapshotIndex: ,
					}
				}
				rf.commitIdx = maxReplicatedIdx
				rf.lastAppliedIdx = maxReplicatedIdx // TODO: ?
			} else {
				rf.lf("leader apply loop: no actions. maxReplicatedIdx: %d rf.commitIdx: %d", maxReplicatedIdx, rf.commitIdx)
			}
		}

		rf.mu.Unlock()
		time.Sleep(heartbeatTimeSpan)
	}
}

// maxReplicatedIdx calculates, based on matchIdx array, the maximum
// replicated index across the majority of follower peers. Returns -1
// if no valid idx is replicated across majority of followers.
func maxReplicatedIdx(me, term, nPeers int, log []LogEntry, matchIdxs []int) int {
	maxMatchIdx := -1
	matchIdxCount := map[int]int{}

	// Build a map to count how many followers
	// have acknowledged each index
	for i, mi := range matchIdxs {
		if i != me && mi > 0 && log[mi-1].Term == term {
			// If one follower has ack one index I
			// that means it has also ack all indexes
			// from 1 -> I
			for j := mi; j >= 1; j-- {
				if _, ok := matchIdxCount[j]; !ok {
					matchIdxCount[j] = 1
				} else {
					matchIdxCount[j] = matchIdxCount[j] + 1
				}
			}
		}
		if mi > maxMatchIdx {
			maxMatchIdx = mi
		}
	}

	// The maximum replicated index is the
	// highest index that at least the majority
	// of followers have acknowledged it
	maxReplicatedIdx := -1
	majority := nPeers / 2 // only counting followers
	for i := maxMatchIdx; i >= 1; i-- {
		if matchIdxCount[i] >= majority {
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
	// TODO: SHould we use as inner mutex the same as raft mu?
	if strings.EqualFold(os.Getenv("RAFT_DBG"), "true") {
		rf.debug = true
	}

	rf.votedFor = -1
	rf.logIdx = 1
	rf.isLeaderCond = *sync.NewCond(&sync.Mutex{})
	rf.electionTimeRef = time.Now()
	rf.applyCh = applyCh

	rf.nextIdx = make([]int, len(peers))
	for i := range peers {
		rf.nextIdx[i] = rf.logIdx // TODO: + 1?
	}
	rf.matchIdx = make([]int, len(peers))

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go rf.startLeaderLoop()

	go rf.startApplyMssgLoop()

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
