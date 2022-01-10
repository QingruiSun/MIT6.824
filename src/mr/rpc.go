package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}


type RegisterArgs struct {

}

type RegisterReply struct {
	WorkerID int
	NReduce  int
}

type RequestTaskArgs struct {
	WorkerID int
}

type RequestTaskReply struct {
	TaskType int
	FileName string
	MapID    int
	ReduceID int
}

type ReportTaskArgs struct {
	TaskType int
	MapID    int
	ReduceID int
	WorkerID int
}

type ReportTaskReply struct {

}

// Add your RPC definitions here.


// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
