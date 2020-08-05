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

const expiredTime = 10

//仅master使用的状态，不涉及网络传输 所以用int
const (
	NotStart = iota
	InMap
	MapDone
	InReduce
	ReduceDone
)

//file
const (
	Idle = iota
	Using
	Done
)

//任务类型 int
const (
	MapType = iota
	ReduceType
)

const (
	Waiting = iota
	Success
	Fail
)

//getWorkerTask的返回状态
const (
	GetMapTaskSuccess    = "getMapTaskSuccess"
	NoMapTask            = "NoMapTask"
	GetReduceTaskSuccess = "GetReduceTaskSuccess"
	NoReduceTask         = "NoReduceTask"
	AllDone              = "AllDone"
)

//WorkerDone的提交状态
const (
	MapTaskDone    = "MapTaskDone"
	MapTaskFail    = "MapTaskFail"
	ReduceTaskDone = "ReduceTaskDone"
	ReduceTaskFail = "ReduceTaskFail"
)

//返回给worker的任务类型标识
const (
	MapTask    = "MapTask"
	ReduceTask = "ReduceTask"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type MapFilenameReq struct {
	Test string
}

type MapFilenameResp struct {
	Filename string
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
