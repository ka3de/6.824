package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

const (
	workerSleep = time.Duration(1) * time.Second
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type OutFile struct {
	Out *os.File
	Enc *json.Encoder
}

type MapOutput struct {
	inited  bool
	taskID  int
	nReduce int
	out     []OutFile
}

func NewMapOutput(taskID, nReduce int) *MapOutput {
	return &MapOutput{
		inited:  false,
		taskID:  taskID,
		nReduce: nReduce,
		out:     make([]OutFile, nReduce),
	}
}

func (o *MapOutput) init() error {
	if o.inited {
		return nil
	}

	for i := 0; i < o.nReduce; i++ {
		oFileName := fmt.Sprintf("mr-%v-%v", o.taskID, i)
		oFile, err := os.Create(oFileName)
		if err != nil {
			return err
		}

		o.out[i] = OutFile{
			Out: oFile,
			Enc: json.NewEncoder(oFile),
		}
	}

	o.inited = true

	return nil
}

func (o *MapOutput) write(kv KeyValue) error {
	if !o.inited {
		if err := o.init(); err != nil {
			return err
		}
	}

	reduceID := ihash(kv.Key) % o.nReduce

	return o.out[reduceID].Enc.Encode(&kv)
}

func (o *MapOutput) closeAll() {
	for _, o := range o.out {
		o.Out.Close()
	}
}

func (o *MapOutput) reset() {
	o.out = make([]OutFile, o.nReduce)
	o.inited = false
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

//
// main/mrworker.go calls this function.
//
func Worker(mapF func(string, string) []KeyValue, reduceF func(string, []string) string) {
	for {
		task, err := requestTask()
		if err != nil {
			// If we can not communicate with
			// the coordinator, let worker die
			return
		}

		switch task.Typ {
		case TaskTypMap:
			err = mapTask(task, mapF)
		case TaskTypReduce:
			err = reduceTask(task, reduceF)
		case TaskTypDone:
			// Job done, worker can die
			return
		default:
			time.Sleep(workerSleep)
			continue
		}

		var result = TaskResult{
			Typ: task.Typ,
			ID:  task.ID,
		}
		if err != nil {
			result.Result = TaskResultFailed
		} else {
			result.Result = TaskResultOk
		}

		_ = reportResult(result) // nolint
	}
}

func mapTask(task Task, f func(string, string) []KeyValue) error {
	mapFileContent, err := readFile(task.MapFile)
	if err != nil {
		return err
	}

	kva := f(task.MapFile, string(mapFileContent))

	out := NewMapOutput(task.ID, task.NReduce)
	defer out.closeAll()
	defer out.reset()

	for _, kv := range kva {
		if err = out.write(kv); err != nil {
			return err
		}
	}

	return nil
}

func reduceTask(task Task, f func(string, []string) string) error {
	var kva []KeyValue

	// Read Key Values from NMaps files for task.ID reduce task
	for i := 0; i < task.NMaps; i++ {
		kvFile, err := os.Open(fmt.Sprintf("mr-%v-%v", i, task.ID))
		if err != nil {
			return err
		}
		defer kvFile.Close()

		dec := json.NewDecoder(kvFile)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
	}

	sort.Sort(ByKey(kva))

	oTmpFileName := fmt.Sprintf("mr-out-tmp-%v", task.ID)
	oTmpFile, err := os.Create(oTmpFileName)
	if err != nil {
		return err
	}

	// Group Key Values by key, apply reduce function and write output
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := f(kva[i].Key, values)

		fmt.Fprintf(oTmpFile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	_ = oTmpFile.Close() //nolint
	return os.Rename(oTmpFileName, fmt.Sprintf("mr-out-%v", task.ID))
}

func requestTask() (Task, error) {
	args := TaskReq{}
	reply := Task{}

	if res := call("Coordinator.RequestTask", &args, &reply); !res {
		return Task{}, ErrCoordinatorTO
	}

	return reply, nil
}

func reportResult(status TaskResult) error {
	ack := TaskAck{}

	if res := call("Coordinator.ReportResult", &status, &ack); !res {
		return ErrCoordinatorTO
	}

	return nil
}

func readFile(filePath string) ([]byte, error) {
	file, err := os.Open(filePath)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	return ioutil.ReadAll(file)
}

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
