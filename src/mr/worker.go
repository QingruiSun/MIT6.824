package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"
import "path/filepath"
import "time"
import "os"
import "io/ioutil"
import "strings"


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

type worker struct {
	workerID int
	mapf     func(string, string) []KeyValue
	reducef  func(string, []string) string
	nReduce  int
	taskType int
	fileName string
	mapID    int
	reduceID int
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	res := call("Master.RegisterWorker", &args, &reply)
	if res == false {
		log.Fatalf("cannot register worker")
	}
	w.workerID = reply.WorkerID
	w.nReduce = reply.NReduce
}

func (w *worker) requestTask() {
	args := RequestTaskArgs{WorkerID: w.workerID}
    reply := RequestTaskReply{}
    res := call("Master.RequestTask", &args, &reply)
    if res == false {
        log.Fatalf("cannot request task")
    }
	w.taskType = reply.TaskType
	if w.taskType == mapTaskType {
		w.mapID = reply.MapID
		w.fileName = reply.FileName
	}
	if w.taskType == reduceTaskType {
		w.reduceID = reply.ReduceID
	}
}


func (w *worker) run() {
	for {
		w.requestTask()
		if w.taskType == mapTaskType {
		    w.doMapTask()
		} else if w.taskType == reduceTaskType {
		    w.doReduceTask()
		} else if w.taskType == waitTaskType {
	        time.Sleep(500 * time.Millisecond)
		} else {
			break
		}
	}
}


func (w *worker) reportTask() {
	args := ReportTaskArgs{w.taskType, w.mapID, w.reduceID, w.workerID}
	reply := ReportTaskReply{}
	res := call("Master.ReportTask", &args, &reply)
	if res == false {
		log.Fatalf("worker report task failed")
	}
}

func (w *worker) doMapTask() {
	file, err := os.Open(w.fileName)
	if err != nil {
		log.Fatalf("cannot open file %v", w.fileName)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", w.fileName)
	}
	file.Close()
	kva := w.mapf(w.fileName, string(content))
	reduces := make([][]KeyValue, w.nReduce)
	for _, kv := range kva {
		idx := ihash(kv.Key) % w.nReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	for idx, l := range reduces {
		fileName := fmt.Sprintf("mr-%d-%d", w.mapID, idx)
		f, err := os.Create(fileName)
		if err != nil {
			log.Fatalf("cannot create temp map files")
		}
		enc := json.NewEncoder(f)
		for _, kv := range l {
			if err := enc.Encode(&kv); err != nil {
				log.Fatalf("cannot encode ")
			}

		}
		if err := f.Close(); err != nil {
			log.Fatalf("cannot encode temp result")
		}
	}
	w.reportTask()
}

func (w *worker) doReduceTask() {
	maps := make(map[string][]string)
	pattern := fmt.Sprintf("./mr-*-%v", w.reduceID)
	fileNames, err := filepath.Glob(pattern)
	if err != nil {
		log.Fatalf("cannot find match files")
	}
	fileNums := len(fileNames)
	for i := 0; i < fileNums; i++ {
		file, err := os.Open(fileNames[i])
		if err != nil {
			log.Fatalf("cannot open intermediate files")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			if _, ok := maps[kv.Key]; !ok {
				maps[kv.Key] = make([]string, 0, 100)
			}
			maps[kv.Key] = append(maps[kv.Key], kv.Value)
		}
	}
	res := make([]string, 0, 100)
	for k, v := range maps {
		res = append(res, fmt.Sprintf("%v %v\n", k, w.reducef(k, v)))
	}
	oname := fmt.Sprintf("mr-out-%d", w.reduceID)
	if err := ioutil.WriteFile(oname, []byte(strings.Join(res, "")), 0600); err != nil {
		log.Fatalf("cannot write reduce file");
	}
	w.reportTask()
}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		w := worker{}
		w.mapf = mapf
		w.reducef = reducef
		w.register()
		w.run()
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
