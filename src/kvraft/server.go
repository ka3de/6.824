package kvraft

import (
	"bytes"
	"fmt"
	"log"
	"os"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	raftTO = 500 * time.Millisecond
)

const Debug = false

const (
	OpGet = iota
	OpPut
	OpAppend
)

type OpType int

func (o OpType) String() string {
	switch o {
	case OpGet:
		return "Get"
	case OpPut:
		return "Put"
	case OpAppend:
		return "Append"
	default:
		return "Unknown"
	}
}

const (
	RfOK = iota
	RfKO
	RfNF
)

type RfRes struct {
	Res   int
	Value string
}

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Type     OpType
	Key      string
	Value    string
	ClientID int64
	ReqID    int64
}

type Watcher struct {
	ExpClientID int64
	ExpReqID    int64
	RfRes       chan<- RfRes
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	debug bool

	state       map[string]string
	lastApplied map[int64]int64
	watchers    map[int]Watcher
}

func (kv *KVServer) Get(args *GetArgs, reply *Reply) {
	kv.lf("received Get RPC call with args: %#v", args)

	op := Op{
		Type:     OpGet,
		Key:      args.Key,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	kv.waitForConsensus(op, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *Reply) {
	kv.lf("received PutAppend RPC call with args: %#v", args)

	op := Op{
		Key:      args.Key,
		Value:    args.Value,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	if args.Op == "Put" {
		op.Type = OpPut
	} else {
		op.Type = OpAppend
	}

	kv.waitForConsensus(op, reply)
}

func (kv *KVServer) waitForConsensus(op Op, reply *Reply) {
	// Start Raft consensus
	rfIdx, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		reply.Err = ErrWrongLeader
		return
	}

	kv.lf("started raft consensus on idx: %v", rfIdx)

	// Add watcher
	rfDone := make(chan RfRes, 1)
	kv.mu.Lock()
	kv.watchers[rfIdx] = Watcher{
		ExpClientID: op.ClientID,
		ExpReqID:    op.ReqID,
		RfRes:       rfDone,
	}
	kv.mu.Unlock()

	// Wait for op to be processed by raft
	kv.l("waiting for raft")
	var rfRes RfRes
	select {
	case rfRes = <-rfDone:
	case <-time.After(raftTO):
		reply.Err = ErrWrongLeader
		return
	}

	switch rfRes.Res {
	case RfOK:
		kv.lf("op type: %s on key: %s was commited successfully", op.Type, op.Key)
		reply.Err = OK
		reply.Value = rfRes.Value
	case RfKO:
		kv.lf("op type: %s on key: %s was not commited due to leader change", op.Type, op.Key)
		reply.Err = ErrWrongLeader
	case RfNF:
		kv.lf("op type: %s on key: %s was commited but key was not found", op.Type, op.Key)
		reply.Err = ErrNoKey
	}
}

func (kv *KVServer) applyLoop() {
	for m := range kv.applyCh {
		if kv.killed() {
			break
		}

		kv.mu.Lock()

		if m.CommandValid {
			var found bool
			var value string

			op := m.Command.(Op)

			if op.Type == OpGet {
				value, found = kv.updateState(op)
			} else {
				// Check if request was already processed
				var lastReq int64
				lastReq, found = kv.lastApplied[op.ClientID]
				if !found || op.ReqID > lastReq {
					value, found = kv.updateState(op)
				}
				kv.lastApplied[op.ClientID] = op.ReqID
			}
			kv.checkWatchers(m.CommandIndex, op, value, found)

			// Handle snapshooting
			if kv.isSnapshotEnabled() && kv.rf.GetStateSize() >= kv.maxraftstate {
				kv.rf.Snapshot(m.CommandIndex, kv.snapshot())
			}
		} else if m.SnapshotValid {
			kv.readSnapshot(m.Snapshot)
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) updateState(op Op) (string, bool) {
	switch op.Type {
	case OpPut:
		kv.state[op.Key] = op.Value
	case OpAppend:
		kv.state[op.Key] += op.Value
	}
	v, ok := kv.state[op.Key]
	return v, ok
}

func (kv *KVServer) checkWatchers(idx int, op Op, val string, found bool) {
	watcher, ok := kv.watchers[idx]
	if !ok {
		return
	}

	var res RfRes
	if watcher.ExpClientID == op.ClientID && watcher.ExpReqID == op.ReqID {
		if found {
			res.Res = RfOK
			res.Value = val
		} else {
			res.Res = RfNF
		}
	} else {
		res.Res = RfKO
	}

	watcher.RfRes <- res
	delete(kv.watchers, idx)
}

func (kv *KVServer) isSnapshotEnabled() bool {
	return kv.maxraftstate != -1
}

func (kv *KVServer) snapshot() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)

	var err error
	if err = e.Encode(kv.state); err != nil {
		kv.lf("error encoding persistent state: %v", err)
		return nil
	}
	if err = e.Encode(kv.lastApplied); err != nil {
		kv.lf("error encoding persistent state: %v", err)
		return nil
	}

	return w.Bytes()
}

func (kv *KVServer) readSnapshot(snapshot []byte) {
	if snapshot == nil || len(snapshot) < 1 {
		return
	}

	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)

	var state map[string]string
	var lastApplied map[int64]int64

	var err error
	if err = d.Decode(&state); err != nil {
		kv.lf("error decoding snapshot: %v", err)
		return
	}
	if err = d.Decode(&lastApplied); err != nil {
		kv.lf("error decoding snapshot: %v", err)
		return
	}

	kv.state = state
	kv.lastApplied = lastApplied
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	if strings.EqualFold(os.Getenv("KVRAFT_DBG"), "true") {
		kv.debug = true
	}

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	kv.state = make(map[string]string)
	kv.lastApplied = make(map[int64]int64)
	kv.watchers = make(map[int]Watcher)

	// initialize from state persisted before a crash
	kv.readSnapshot(persister.ReadSnapshot())

	go kv.applyLoop()

	return kv
}

// l logs the given string s for the kv peer.
func (kv *KVServer) l(s string) {
	if kv.debug {
		defColor := "\033[0m"
		color := fmt.Sprintf("\033[%vm", 31+kv.me)
		prefix := fmt.Sprintf("%s[PEER %d] ", color, kv.me)
		log.Print(prefix, s, defColor)
	}
}

// lf logs the given string format with fields values for the kv peer.
func (kv *KVServer) lf(format string, fields ...interface{}) {
	kv.l(fmt.Sprintf(format, fields...))
}
