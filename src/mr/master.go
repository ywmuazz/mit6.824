package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MasterState int
type SplitStat int
type MapData map[string]interface{}

//未防止中途任务失败，所以没有任务不代表所有任务已完成，需要轮询继续等待alldone

type Master struct {
	// Your definitions here.
	filenames []string
	nReduce   int

	numSplitsInMap int
	mapFiles       []string
	inMapFiles     []string
	doneMapFiles   []string

	reduceFiles     []string
	inReduceFiles   []string
	doneReduceFiles []string
	mSplitsState    map[string]SplitStat

	start bool
	done  bool
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
	ret := m.done
	// Your code here.

	return ret
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	mp := map[string]SplitStat{}
	for _, f := range files {
		_, ok := mp[f]
		if !ok {
			mp[f] = InMap
		}
	}
	m := Master{filenames: files,
		nReduce:        nReduce,
		numSplitsInMap: len(mp),
		mSplitsState:   mp,
		done:           false,
	}

	// Your code here.

	m.server()
	return &m
}

func (m *Master) getNumMapFiles() int    { return len(m.mapFiles) }
func (m *Master) getNumReduceFiles() int { return len(m.reduceFiles) }

func (m *Master) getNumAllMapTask() int  { return len(m.filenames) }
func (m *Master) getNumInMapTask() int   { return len(m.inMapFiles) }
func (m *Master) getNumDoneMapTask() int { return len(m.doneMapFiles) }
func (m *Master) isMapAllDone() bool     { return m.getNumDoneMapTask() == m.getNumAllMapTask() }

func (m *Master) getNumAllReduceTask() int  { return m.getNumAllMapTask() }
func (m *Master) getNumInReduceTask() int   { return len(m.inReduceFiles) }
func (m *Master) getNumDoneReduceTask() int { return len(m.doneReduceFiles) }
func (m *Master) isReduceAllDone() bool     { return m.getNumDoneReduceTask() == m.getNumAllReduceTask() }

func (m *Master) getState() MasterState {
	if !m.start {
		return NotStart
	} else if !m.isMapAllDone() {
		return InMap
	} else if !m.isReduceAllDone() {
		return InReduce
	} else {
		return ReduceDone
	}

}

func (m *Master) WorkResult(req *MapData, resp *MapData) error {
	state := m.getState()
	succ := req.GetString("success")
	inputFile := req.GetString("inputFile")
	outputFile := req.GetString("outputFile")
	fmt.Println("master get workResult: ", succ)
	if succ == MapTaskDone {
		fmt.Printf("inputfile: %v outputfile: %v\n", inputFile, outputFile)
		first = true
	} else if succ == MapTaskFail {

	} else if succ == ReduceTaskDone {
		fmt.Printf("inputfile: %v outputfile: %v\n", inputFile, outputFile)
		m.done = true
	} else if succ == ReduceTaskFail {

	} else {
		fmt.Println("not accepted success state.", state, inputFile, outputFile)
	}
	return nil
}

func (m *Master) WorkerDone(req *WorkerDoneReq, resp *WorkerDoneResp) error {
	m.done = true
	resp.Success = true
	return nil
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) GetMapFilename(req *MapFilenameReq, resp *MapFilenameResp) error {
	//说实话要不要检测失败的任务，重新发出去
	resp.Filename = req.Test
	return nil
}

var (
	first = false
)

func (m *Master) GetWorkerTask(req *MapData, resp *MapData) error {
	mp := *resp
	if m.done {
		mp["success"] = AllDone
	} else if first == false {
		mp["success"] = GetMapTaskSuccess
		mp["taskType"] = MapTask
		mp["inputFile"] = "filemap"
	} else {
		mp["success"] = GetReduceTaskSuccess
		mp["taskType"] = ReduceTask
		mp["inputFile"] = "filereduce"
	}

	return nil
}
