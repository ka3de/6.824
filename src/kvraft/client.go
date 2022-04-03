package kvraft

import (
	"crypto/rand"
	"fmt"
	"log"
	"math/big"
	"os"
	"strings"
	"sync/atomic"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	debug bool

	leaderID int32
	clientID int64
	reqID    int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	if strings.EqualFold(os.Getenv("KVRAFT_DBG"), "true") {
		ck.debug = true
	}
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.lf("executing Get for k:%s", key)

	args := GetArgs{
		ClientID: ck.clientID,
		ReqID:    atomic.AddInt64(&ck.reqID, 1),
		Key:      key,
	}

	for i := atomic.LoadInt32(&ck.leaderID); ; i++ {
		reply := Reply{}
		srv := i % int32(len(ck.servers))

		ok := ck.servers[srv].Call("KVServer.Get", &args, &reply)

		if ok {
			ck.lf("srv %d returned err: %s value: %v", srv, reply.Err, reply.Value)
			if reply.Err == OK {
				ck.leaderID = i
				return reply.Value
			} else if reply.Err == ErrNoKey {
				ck.leaderID = i
				return ""
			}
		} else {
			ck.lf("srv %d could not be reached", srv)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.lf("executing %s for k:%s v:%s", op, key, value)

	args := PutAppendArgs{
		ClientID: ck.clientID,
		ReqID:    atomic.AddInt64(&ck.reqID, 1),
		Op:       op,
		Key:      key,
		Value:    value,
	}

	for i := atomic.LoadInt32(&ck.leaderID); ; i++ {
		reply := Reply{}
		srv := i % int32(len(ck.servers))

		ok := ck.servers[srv].Call("KVServer.PutAppend", &args, &reply)

		if ok {
			ck.lf("srv %d returned %s", srv, reply.Err)
			if reply.Err == OK {
				ck.leaderID = i
				return
			}
		} else {
			ck.lf("srv %d could not be reached", srv)
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}

// l logs the given string s for.
func (ck *Clerk) l(s string) {
	if ck.debug {
		log.Print("[CLIENT] ", s)
	}
}

// lf logs the given string format with fields values.
func (ck *Clerk) lf(format string, fields ...interface{}) {
	ck.l(fmt.Sprintf(format, fields...))
}
