package mr

import "log"
import "net"
import "os"
import "sync"
import "time"
import "net/rpc"
import "net/http"
import "fmt"


const (
    idle = 0
    progress = 1
	completed = 2
	mapTaskType = 0
	reduceTaskType = 1
	waitTaskType = 2
	doneTaskType = 3
	mapPhase = 0
	reducePhase = 1
	waitMapPhase = 2
	waitReducePhase = 3
	donePhase = 4
)


type Task struct {
	taskType  int
	fileName  string
	mapID     int
	reduceID  int
	state     int
	startTime time.Time
}


type Master struct {
	mu              sync.Mutex
	mapTasks        []Task
	reduceTasks     []Task
	workerSeq       int
	nReduce         int
	nMap            int
	allocatedMap    int
	completedMap    int
	allocatedReduce int
	completedReduce int
	taskPhase       int
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


func (m *Master) RegisterWorker(args *RegisterArgs, reply *RegisterReply) error {
    m.mu.Lock()
	m.workerSeq++
	reply.WorkerID = m.workerSeq
	m.mu.Unlock()
	reply.NReduce = m.nReduce
	fmt.Println("register in master")
	return nil
}

func (m *Master) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if m.taskPhase == mapPhase {
		for i := range m.mapTasks {
			if m.mapTasks[i].state == idle {
				reply.TaskType = mapTaskType
				reply.MapID = m.mapTasks[i].mapID
				reply.FileName = m.mapTasks[i].fileName
				m.mapTasks[i].state = progress
				m.mapTasks[i].startTime = time.Now()
				m.allocatedMap++
				if m.allocatedMap == m.nMap {
					m.taskPhase = waitMapPhase
				}
				break
			}
		}
	} else if m.taskPhase == waitMapPhase {
		reply.TaskType = waitTaskType
	} else if m.taskPhase == reducePhase {
		for i := range m.reduceTasks {
			if m.reduceTasks[i].state == idle {
				reply.TaskType = reduceTaskType
				reply.ReduceID = i
				m.reduceTasks[i].state = progress
				m.reduceTasks[i].startTime = time.Now()
				m.allocatedReduce++
				if m.allocatedReduce == m.nReduce {
					m.taskPhase = waitReducePhase
				}
				break
			}
		}
	} else if m.taskPhase == waitReducePhase {
		reply.TaskType = waitTaskType
	} else {
		reply.TaskType = doneTaskType
	}
	return nil
}

func (m *Master) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	if args.TaskType == mapTaskType {
		m.mu.Lock()
		m.mapTasks[args.MapID].state = completed
		m.completedMap++
		if m.completedMap == m.nMap {
			m.taskPhase = reducePhase
		}
		m.mu.Unlock()
	} else if args.TaskType == reduceTaskType {
		m.mu.Lock()
		m.reduceTasks[args.ReduceID].state = completed
		m.completedReduce++
		if m.completedReduce == m.nReduce {
			m.taskPhase = donePhase
		}
		m.mu.Unlock()
	}
	return nil
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
	if m.taskPhase == donePhase {
		ret = true
	}
	return ret
}

func (m *Master) schedule() {
	for {
		time.Sleep(1000 * time.Millisecond)
		m.mu.Lock()
		if (m.taskPhase == mapPhase) || (m.taskPhase == waitMapPhase) {
			for i := 0; i < len(m.mapTasks); i++ {
				if (m.mapTasks[i].state == progress) && (time.Now().Sub(m.mapTasks[i].startTime).Seconds() >= 10) {
					m.mapTasks[i].state = idle
					m.allocatedMap--
				}
			}
			if m.allocatedMap < m.nMap {
				m.taskPhase = mapPhase
			}
		}
		if (m.taskPhase == reducePhase) || (m.taskPhase == waitReducePhase) {
			for i := 0; i < len(m.reduceTasks); i++ {
				if (m.reduceTasks[i].state == progress) && (time.Now().Sub(m.reduceTasks[i].startTime).Seconds() >= 10) {
					m.reduceTasks[i].state = idle
					m.allocatedReduce--
				}
			}
			if m.allocatedReduce < m.nReduce {
				m.taskPhase = reducePhase
			}
		}
		m.mu.Unlock()
	}
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
		mapTask := Task{taskType: mapTaskType, fileName: files[i], mapID: i, state: idle}
		m.mapTasks = append(m.mapTasks, mapTask)
	}
	m.nReduce = nReduce
	m.nMap = len(files)
	for i := 0; i < nReduce; i++ {
		reduceTask := Task{taskType: reduceTaskType, reduceID: i, state: idle}
		m.reduceTasks = append(m.reduceTasks, reduceTask)
	}
	m.mu.Unlock()
	m.server()
	go m.schedule()
	return &m
}
