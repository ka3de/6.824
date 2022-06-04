package shardctrler

import (
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"sync"
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
	OpJoin = iota
	OpLeave
	OpMove
	OpQuery
)

type OpType int

func (o OpType) String() string {
	switch o {
	case OpJoin:
		return "Join"
	case OpLeave:
		return "Leave"
	case OpMove:
		return "Move"
	case OpQuery:
		return "Query"
	default:
		return "Unknown"
	}
}

type Op struct {
	// Your data here.
	Type     OpType
	Args     interface{}
	ClientID int64
	ReqID    int64
}

type Reply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}

type Watcher struct {
	ExpClientID int64
	ExpReqID    int64
	ReplyCh     chan<- Reply
}

// Your sharded key/value store will have two main components. First, a set of
// replica groups. Each replica group is responsible for a subset of the shards.
// A replica consists of a handful of servers that use Raft to replicate the group's
// shards. The second component is the "shard controller". The shard controller
// decides which replica group should serve each shard; this information is called
// the configuration. The configuration changes over time. Clients consult the shard
// controller in order to find the replica group for a key, and replica groups consult
// the controller in order to find out what shards to serve. There is a single shard
// controller for the whole system, implemented as a fault-tolerant service using Raft.

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	debug bool

	lastApplied map[int64]int64
	watchers    map[int]Watcher

	configs []Config // indexed by config num
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.lf("received Join RPC call with args: %#v", args)

	op := Op{
		Type:     OpJoin,
		Args:     *args,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	rfReply := sc.waitForConsensus(op)

	reply.WrongLeader = rfReply.WrongLeader
	reply.Err = rfReply.Err
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.lf("received Leave RPC call with args: %#v", args)

	op := Op{
		Type:     OpLeave,
		Args:     *args,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	rfReply := sc.waitForConsensus(op)

	reply.WrongLeader = rfReply.WrongLeader
	reply.Err = rfReply.Err
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.lf("received Move RPC call with args: %#v", args)

	op := Op{
		Type:     OpMove,
		Args:     *args,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	rfReply := sc.waitForConsensus(op)

	reply.WrongLeader = rfReply.WrongLeader
	reply.Err = rfReply.Err
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	sc.lf("received Query RPC call with args: %#v", args)

	op := Op{
		Type:     OpQuery,
		Args:     *args,
		ClientID: args.ClientID,
		ReqID:    args.ReqID,
	}

	rfReply := sc.waitForConsensus(op)

	reply.WrongLeader = rfReply.WrongLeader
	reply.Err = rfReply.Err
	reply.Config = rfReply.Config
}

func (sc *ShardCtrler) waitForConsensus(op Op) Reply {
	// Start Raft consensus
	rfIdx, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		sc.l("peer is not leader")
		return Reply{WrongLeader: true}
	}

	sc.lf("started raft consensus on idx: %v", rfIdx)

	// Add watcher
	replyCh := make(chan Reply, 1)
	sc.mu.Lock()
	sc.watchers[rfIdx] = Watcher{
		ExpClientID: op.ClientID,
		ExpReqID:    op.ReqID,
		ReplyCh:     replyCh,
	}
	sc.mu.Unlock()

	// Wait for op to be processed by raft
	sc.l("waiting for raft")
	select {
	case rfRes := <-replyCh:
		return rfRes
	case <-time.After(raftTO):
		return Reply{WrongLeader: true}
	}
}

func (sc *ShardCtrler) applyLoop() {
	for m := range sc.applyCh {
		sc.mu.Lock()

		if m.CommandValid {
			sc.lf("raft applied Op: %v", m.Command)

			var config Config
			op := m.Command.(Op)

			if op.Type == OpQuery {
				config = sc.updateState(op)
			} else {
				// Check if request was already processed
				lastReq, found := sc.lastApplied[op.ClientID]
				if !found || op.ReqID > lastReq {
					config = sc.updateState(op)
					sc.lastApplied[op.ClientID] = op.ReqID
				}
			}
			sc.checkWatchers(m.CommandIndex, op, config)

		}

		sc.mu.Unlock()
	}
}

func (sc *ShardCtrler) updateState(op Op) Config {
	var config Config

	switch op.Type {
	case OpJoin:
		if joinArgs, ok := op.Args.(JoinArgs); ok {
			config = sc.handleJoinOp(joinArgs)
		}
	case OpLeave:
		if leaveArgs, ok := op.Args.(LeaveArgs); ok {
			config = sc.handleLeaveOp(leaveArgs)
		}
	case OpMove:
		if moveArgs, ok := op.Args.(MoveArgs); ok {
			config = sc.handleMoveOp(moveArgs)
		}
	case OpQuery:
		if queryArgs, ok := op.Args.(QueryArgs); ok {
			config = sc.handleQueryOp(queryArgs)
		}
	default:
		config = Config{}
	}

	return config
}

func (sc *ShardCtrler) handleJoinOp(args JoinArgs) Config {
	newConfig := sc.newConfig()

	// Copy new groups configuration to new config
	for gID, srvrs := range args.Servers {
		if _, ok := newConfig.Groups[gID]; !ok {
			newConfig.Groups[gID] = srvrs
		} else {
			newConfig.Groups[gID] = append(newConfig.Groups[gID], srvrs...)
		}
	}
	newConfig.Shards = balanceShards(newConfig.Groups)
	sc.configs = append(sc.configs, newConfig)

	return newConfig
}

func (sc *ShardCtrler) handleLeaveOp(args LeaveArgs) Config {
	newConfig := sc.newConfig()

	// Remove leaving gIDs from current configuration
	for _, gID := range args.GIDs {
		delete(newConfig.Groups, gID)
	}
	newConfig.Shards = balanceShards(newConfig.Groups)
	sc.configs = append(sc.configs, newConfig)

	return newConfig
}

func (sc *ShardCtrler) handleMoveOp(args MoveArgs) Config {
	newConfig := sc.newConfig()

	// Copy current shards mapping config
	newConfig.Shards = sc.configs[len(sc.configs)-1].Shards
	// Set requested shard->gID configuration
	newConfig.Shards[args.Shard] = args.GID
	sc.configs = append(sc.configs, newConfig)

	return newConfig
}

func (sc *ShardCtrler) handleQueryOp(args QueryArgs) Config {
	var config Config

	// Get the requested config
	if args.Num < 0 || args.Num >= len(sc.configs) {
		config = sc.configs[len(sc.configs)-1]
	} else {
		config = sc.configs[args.Num]
	}

	return config
}

func (sc *ShardCtrler) checkWatchers(idx int, op Op, config Config) {
	watcher, ok := sc.watchers[idx]
	if !ok {
		return
	}

	var reply Reply
	if watcher.ExpClientID == op.ClientID && watcher.ExpReqID == op.ReqID {
		reply.Config = config
	} else {
		reply.WrongLeader = true
	}

	watcher.ReplyCh <- reply
	delete(sc.watchers, idx)
}

// newConfig builds a new Config struct based on the current config.
// Shards mapping is returned as nil and rebalancing should be done.
func (sc *ShardCtrler) newConfig() Config {
	curConfigID := len(sc.configs) - 1

	var newConfig Config
	newConfig.Num = curConfigID + 1
	newConfig.Groups = make(map[int][]string)

	for gID, srvrs := range sc.configs[curConfigID].Groups {
		newConfig.Groups[gID] = make([]string, len(srvrs))
		copy(newConfig.Groups[gID], srvrs)
	}

	return newConfig
}

// balanceShards returns an evenly distributed shard->gID mapping.
func balanceShards(groups map[int][]string) [NShards]int {
	var shardsMapping [10]int

	var gIDs []int
	for gID := range groups {
		gIDs = append(gIDs, gID)
	}
	sort.Slice(gIDs, func(i, j int) bool {
		return gIDs[i] < gIDs[j]
	})

	if len(gIDs) == 0 {
		for i := 0; i < NShards; i++ {
			shardsMapping[i] = 0
		}
	} else {
		for i := 0; i < NShards; i++ {
			shardsMapping[i] = gIDs[i%len(gIDs)]
		}
	}

	return shardsMapping
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	sc.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})

	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)

	// Your code here.
	if strings.EqualFold(os.Getenv("SHARD_CTRLR_DBG"), "true") {
		sc.debug = true
	}
	sc.lastApplied = make(map[int64]int64)
	sc.watchers = make(map[int]Watcher)

	go sc.applyLoop()

	return sc
}

// l logs the given string s for the sc peer.
func (sc *ShardCtrler) l(s string) {
	if sc.debug {
		defColor := "\033[0m"
		color := fmt.Sprintf("\033[%vm", 31+sc.me)
		prefix := fmt.Sprintf("%s[CTRLR %d] ", color, sc.me)
		log.Print(prefix, s, defColor)
	}
}

// lf logs the given string format with fields values for the sc peer.
func (sc *ShardCtrler) lf(format string, fields ...interface{}) {
	sc.l(fmt.Sprintf(format, fields...))
}
