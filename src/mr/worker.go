package mr

import "fmt"
import "log"
import "net/rpc"
import "hash/fnv"
import "encoding/json"


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
	filename string
	nReduce  int
	reduceID int
}

func (w *worker) register() {
	args := RegisterArgs{}
	reply := RegisterReply{}
	res := call("Master.RegisterWorker", &args, &reply)
	if res == false {
		log.Fatalf("cannot register worker")
	}
	w.workerID = reply.workerID
	w.nReduce = reply.nReduce
}

func (w *worker) requestTask() {
	args := RequestTaskArgs{}
    reply := RequestTaskReply{}
    res := call("Master.RequestTask", &args, &reply)
    if res == false {
        log.Fatalf("cannot request task")
    }
    w.filename = reply.filename
}


func (w *worker) run() {
	
}

func (w *worker) doMapTask() {
	file, err := os.Open(w.filename)
	if err != nil {
		log.Fatalf("cannot open file %v", w.filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kva := w.mapf(w.filename, string(content))
	intermediateFiles := make([]os.*File, w.nReduce)
	prefix := fmt.Sprintf("mr-%v", w.workerID)
	for i := 0; i < w.nReduce; ++i {
		fileName := fmt.Sprintf("%v-*", prefix)
		intermediateFiles[i] = os.ioutil.TempFile("", fileName)
	}
	encs := make([]*Encoder, w.nReduce)
	for i := 0; i < w.nReduce; ++i {
		encs[i] = json.NewEncoder(intermediateFiles[i])
	}
	for _, item := range kva {
		index := ihash(item.Key)
		err := encs[index].Encode(&item)
		if err != nil {
			log.Fatalf("cannot encode %v", item)
		}
	}
	for i := 0; i < w.nReduce; ++i {
		intermediateFiles[i].Close()
    }
	for i := 0; i < w.nReduce; ++i {
		fileName := fmt.Sprintf("%v-%v", prefix, i)
		os.Rename(intermediateFiles[i].Name(), fileName)
	}
}

func (w *worker) doReduceTask() {

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
		w := worker(-1, mapf, refucef)
		w.register()

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
