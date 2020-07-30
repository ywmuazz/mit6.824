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

	mapFiles     []string
	inMapFiles   []string
	doneMapFiles []string

	reduceFiles     []string
	inReduceFiles   []string
	doneReduceFiles []string
	mSplitsState    map[string]SplitStat
	mapFileToNo     map[string]int
	reduceFileToNo  map[string]int

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

	m := Master{filenames: files,
		nReduce:         nReduce,
		mapFiles:        []string{},
		inMapFiles:      []string{},
		doneMapFiles:    []string{},
		reduceFiles:     []string{},
		inReduceFiles:   []string{},
		doneReduceFiles: []string{},
		mSplitsState:    map[string]SplitStat{},
		mapFileToNo:     map[string]int{},
		reduceFileToNo:  map[string]int{},
		start:           false,
		done:            false,
	}
	for i, f := range files {
		m.pushMapFile(f)
		m.mapFileToNo[f] = i
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

func (m *Master) pushMapFile(f string) {
	m.mapFiles = append(m.mapFiles, f)
	m.mSplitsState[f] = Idle
}

func (m *Master) popMapFile() string {
	if len(m.mapFiles) == 0 {
		return ""
	}
	ret := m.mapFiles[0]
	m.mapFiles = m.mapFiles[1:]
	return ret
}

func (m *Master) pushInMapFile(f string) {
	m.inMapFiles = append(m.inMapFiles, f)
	m.mSplitsState[f] = Using
}

// func addMapFile(m MapData, f string) {
// 	m["inputfile"] = f
// }

func (m *Master) getMapTask() (MapData, bool) {
	//加锁
	ret := MapData{}
	if m.isMapAllDone() {
		return MapData{}, false
	}
	mapFile := m.popMapFile()
	if mapFile == "" {
		return MapData{}, false
	}

	ret["inputFile"] = mapFile
	no, ok := m.mapFileToNo[mapFile]
	if !ok {
		log.Fatalf("cannot find file %v 's no.", mapFile)
	}
	ret["inputFileNo"] = no
	ret["nReduce"] = m.nReduce
	m.pushInMapFile(mapFile)
	return ret, true
}

//TODO
func (m *Master) getReduceTask() string {
	return ""
}

func (m *Master) WorkerDone(req *WorkerDoneReq, resp *WorkerDoneResp) error {
	m.done = true
	resp.Success = true
	return nil
}

var (
	first = false
)

// GetMapTaskSuccess    = "getMapTaskSuccess"
// NoMapTask            = "NoMapTask"
// GetReduceTaskSuccess = "GetReduceTaskSuccess"
// NoReduceTask         = "NoReduceTask"
// AllDone              = "AllDone"
//完成getTask的逻辑
func (m *Master) GetWorkerTask(req *MapData, resp *MapData) error {
	mp := MapData{}
	state := m.getState()
	if state == NotStart {
		m.start = true
		state = m.getState()
	}
	if state == InMap {
		//加锁？计时？
		//如果没有map任务则在get里面处理出无map任务的返回值
		var ok bool
		mp, ok = m.getMapTask()
		if ok {
			mp["success"] = GetMapTaskSuccess
		} else {
			mp["success"] = NoMapTask
		}

	} else if state == InReduce {
		//TODO
		// mp := m.getReduceTask()
	} else {
		mp["success"] = AllDone
	}
	*resp = mp
	log.Println("getWorkerTask resp: ", *resp)

	// if m.done {
	// 	mp["success"] = AllDone
	// } else if first == false {
	// 	mp["success"] = GetMapTaskSuccess
	// 	mp["taskType"] = MapTask
	// 	mp["inputFile"] = "filemap"
	// } else {
	// 	mp["success"] = GetReduceTaskSuccess
	// 	mp["taskType"] = ReduceTask
	// 	mp["inputFile"] = "filereduce"
	// }

	return nil
}

//~~~~~~~~~~~
//-----------
//###########

func (m *Master) GetMapFilename(req *MapFilenameReq, resp *MapFilenameResp) error {
	//说实话要不要检测失败的任务，重新发出去
	resp.Filename = req.Test
	return nil
}
