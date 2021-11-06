package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"errors"
	"os"
	"strconv"
)

var (
	ErrCoordinatorTO = errors.New("coordinator timeout")
)

const (
	TaskTypMap = iota
	TaskTypReduce
	TaskTypWait
	TaskTypDone
)

type TaskTyp int

type Task struct {
	Typ TaskTyp
	ID  int

	MapFile string

	NMaps   int
	NReduce int
}

const (
	TaskResultOk = iota
	TaskResultFailed
)

type TaskResult struct {
	Typ    TaskTyp
	ID     int
	Result int
}

type TaskReq struct{}

type TaskAck struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
