package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

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

// Add your RPC definitions here.

type RequestTaskArgs struct {
	TaskId  int      // TaskId of finished task. 0 if requesting the first task.
	Outputs []string // Outputs of finished task. Empty if requesting the first task.
}

type RequestTaskReply struct {
	Map        *MapTask
	Reduce     *ReduceTask
	ExitSignal bool
}

type MapTask struct {
	Id                int
	InputFile         string
	NReduce           int
	OutputFilenameFmt string
}

type ReduceTask struct {
	Id             int
	InputFiles     []string
	OutputFilename string
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
