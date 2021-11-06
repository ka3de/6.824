package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

const (
	TaskStatusNotStarted = iota
	TaskStatusAssigned
	TaskStatusFinished
)

type TaskStatus int

type Coordinator struct {
	mux sync.Mutex

	inputFiles []string

	MapTasks    []TaskStatus
	ReduceTasks []TaskStatus

	NMapDone    int
	NReduceDone int

	IsJobDone bool
}

func (c *Coordinator) RequestTask(args *TaskReq, reply *Task) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if c.NMapDone < len(c.MapTasks) {
		return c.assignMapTask(reply)
	}
	if c.NReduceDone < len(c.ReduceTasks) {
		return c.assignReduceTask(reply)
	}

	reply.Typ = TaskTypDone
	return nil
}

func (c *Coordinator) assignMapTask(reply *Task) error {
	for i, mt := range c.MapTasks {
		if mt == TaskStatusNotStarted {
			reply.ID = i
			reply.Typ = TaskTypMap
			reply.MapFile = c.inputFiles[i]
			reply.NMaps = len(c.MapTasks)
			reply.NReduce = len(c.ReduceTasks)

			c.MapTasks[i] = TaskStatusAssigned

			go c.watchWorker(TaskTypMap, i)

			return nil
		}
	}

	// All map tasks have started but not all are
	// completed, so tell worker to wait
	reply.Typ = TaskTypWait
	return nil
}

func (c *Coordinator) assignReduceTask(reply *Task) error {
	for i, rt := range c.ReduceTasks {
		if rt == TaskStatusNotStarted {
			reply.ID = i
			reply.Typ = TaskTypReduce
			reply.NMaps = len(c.MapTasks)
			reply.NReduce = len(c.ReduceTasks)

			c.ReduceTasks[i] = TaskStatusAssigned

			go c.watchWorker(TaskTypReduce, i)

			return nil
		}
	}

	// All reduce tasks have started but not all are
	// completed, so tell worker to wait
	reply.Typ = TaskTypWait
	return nil
}

func (c *Coordinator) ReportResult(res *TaskResult, reply *TaskAck) error {
	c.mux.Lock()
	defer c.mux.Unlock()

	if res.Typ == TaskTypMap {
		return c.handleMapResult(res)
	}
	if res.Typ == TaskTypReduce {
		return c.handleReduceResult(res)
	}
	return nil
}

func (c *Coordinator) handleMapResult(res *TaskResult) error {
	if res.Result == TaskResultOk {
		c.MapTasks[res.ID] = TaskStatusFinished
		c.NMapDone++
	} else if res.Result == TaskResultFailed {
		c.MapTasks[res.ID] = TaskStatusNotStarted
	}

	return nil
}

func (c *Coordinator) handleReduceResult(res *TaskResult) error {
	if res.Result == TaskResultOk {
		c.ReduceTasks[res.ID] = TaskStatusFinished
		c.NReduceDone++

		if c.NReduceDone == len(c.ReduceTasks) {
			c.IsJobDone = true
		}
	} else if res.Result == TaskResultFailed {
		c.ReduceTasks[res.ID] = TaskStatusNotStarted
	}

	return nil
}

// watchWorker waits for the workerID worker for the taskTyp task
// type to do its job. If after the given time the task is not done
// the task status is reset to TaskStatusNotStarted.
// This method should be called inside a separate goroutine to not
// block main coordinator goroutine execution.
func (c *Coordinator) watchWorker(taskTyp TaskTyp, workerID int) {
	time.Sleep(10 * time.Second)

	c.mux.Lock()
	defer c.mux.Unlock()

	var status []TaskStatus
	if taskTyp == TaskTypMap {
		status = c.MapTasks
	} else {
		status = c.ReduceTasks
	}

	if status[workerID] != TaskStatusFinished {
		status[workerID] = TaskStatusNotStarted
	}
}

//
// start a thread that listens for RPCs from worker.go
//
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	c.mux.Lock()
	defer c.mux.Unlock()

	done := c.IsJobDone
	return done
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		inputFiles:  files,
		MapTasks:    make([]TaskStatus, len(files)),
		ReduceTasks: make([]TaskStatus, nReduce),
	}

	for i := range c.MapTasks {
		c.MapTasks[i] = TaskStatusNotStarted
	}
	for i := range c.ReduceTasks {
		c.ReduceTasks[i] = TaskStatusNotStarted
	}

	c.server()
	return &c
}
