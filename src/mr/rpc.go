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

const (
	MapTaskType = iota
	ReduceTaskType
	NoneTaskType
)

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

type GetTaskArgs struct {
}

type GetTaskReply struct {
	TaskType      int
	MapTaskNum    int32
	ReduceTaskNum int32
	Task          Task
}

type Task struct {
	FileName     string
	MapTaskNo    int32
	ReduceTaskNo int32
}

type Finish struct {
	StartTime int64
	Finish    bool
}

type TaskFinishNotifyArgs struct {
	TaskType  int
	CurTaskNo int32
}

type TaskFinishNotifyReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
