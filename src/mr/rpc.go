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

const (
	NotStart = iota
	InMap
	MapDone
	InReduce
	ReduceDone
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

type WorkerDoneReq struct {
}
type WorkerDoneResp struct {
	Success bool
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
