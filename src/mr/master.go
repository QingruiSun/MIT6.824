package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"


const (
    idle = 0
    progress = 1
	completed = 2
	mapTaskType = 0
	reduceTaskType = 1
)

type MapTask struct {
	filename string
	state    int
	workerID int

}


type Master struct {
	mu        sync.Mutex
	mapTasks  []MapTask
	workerSeq int
	nReduce   int
	nMap      int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) {
    m.mu.Lock()
	m.workerSeq++
	reply.workerID = m.workerSeq
	m.mu.Unlock()
	reply.nReduce = m.nReduce
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) {
  
}

//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.


	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}
	m.mu.Lock();
	for i := range files {
		mapTask = MapTask{files[i], idle, -1};
		m.mapTasks = append(m.mapTasks, mapTask)
	}
    m.nReduce = nReduce
	m.nMap = len(files)
	m.mu.Unlock()
	m.server()
	return &m
}
