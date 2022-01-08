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
    workerID int
	nReduce  int
}

type RequestTaskArgs struct {
    workerID int
}

type RequestTaskReply struct {
	taskType int
    fileName string
	mapID    int
	reduceID int
}

type ReportTaskArgs struct {
	taskType int
	mapID    int
	reduceID int
	workerID int
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
