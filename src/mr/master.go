package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type MasterState int
type SplitStat int

//未防止中途任务失败，所以没有任务不代表所有任务已完成，需要轮询继续等待alldone

type Master struct {
	// Your definitions here.
	filenames []string
	nReduce   int
	nMap      int

	mapFiles     []string
	inMapFiles   []string
	doneMapFiles []string

	reduceTasks      []int
	inReduceTasks    []int
	doneReduceTasks  []int
	mSplitsState     map[string]SplitStat
	mReduceTaskState map[int]SplitStat
	mapFileToNo      map[string]int
	reduceTaskToNo   map[string]int

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
	//最好对files去重/检验是否存在
	m := Master{filenames: files,
		nReduce:          nReduce,
		nMap:             len(files),
		mapFiles:         []string{},
		inMapFiles:       []string{},
		doneMapFiles:     []string{},
		reduceTasks:      []int{},
		inReduceTasks:    []int{},
		doneReduceTasks:  []int{},
		mSplitsState:     map[string]SplitStat{},
		mReduceTaskState: map[int]SplitStat{},
		mapFileToNo:      map[string]int{},
		reduceTaskToNo:   map[string]int{},
		start:            false,
		done:             false,
	}
	for i, f := range files {
		m.pushMapFile(f)
		m.mapFileToNo[f] = i
	}
	for i := 0; i < nReduce; i++ {
		m.pushReduceTask(i)
	}

	// Your code here.

	m.server()
	return &m
}

func (m *Master) getNumMapFiles() int    { return len(m.mapFiles) }
func (m *Master) getNumReduceFiles() int { return len(m.reduceTasks) }

func (m *Master) getNumAllMapTask() int  { return len(m.filenames) }
func (m *Master) getNumInMapTask() int   { return len(m.inMapFiles) }
func (m *Master) getNumDoneMapTask() int { return len(m.doneMapFiles) }
func (m *Master) isMapAllDone() bool     { return m.getNumDoneMapTask() == m.getNumAllMapTask() }

func (m *Master) getNumAllReduceTask() int  { return m.nReduce }
func (m *Master) getNumInReduceTask() int   { return len(m.inReduceTasks) }
func (m *Master) getNumDoneReduceTask() int { return len(m.doneReduceTasks) }
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

//删除一个inMapfile
func (m *Master) eraseInMapFileByName(f string) error {
	idx := -1
	for i, _ := range m.inMapFiles {
		if f == m.inMapFiles[i] {
			idx = i
			break
		}
	}
	if idx == -1 {
		return errors.New("cannot find file " + f + " in inMapFiles.")
	}
	m.inMapFiles = append(m.inMapFiles[:idx], m.inMapFiles[idx+1:]...)
	return nil
}

func (m *Master) pushDoneMapFile(f string) {
	m.doneMapFiles = append(m.doneMapFiles, f)
	m.mSplitsState[f] = Done
}

func (m *Master) eraseInReduceTaskByNo(taskNo int) error {
	idx := -1
	for i, _ := range m.inReduceTasks {
		if taskNo == m.inReduceTasks[i] {
			idx = i
			break
		}
	}
	if idx == -1 {
		return errors.New(fmt.Sprintf("cannot find reduceTask %v in inReduceTasks.", taskNo))
	}
	m.inReduceTasks = append(m.inReduceTasks[:idx], m.inReduceTasks[idx+1:]...)
	return nil
}
func (m *Master) pushDoneReduceTask(taskNo int) {
	m.doneReduceTasks = append(m.doneReduceTasks, taskNo)
	m.mReduceTaskState[taskNo] = Done
}

func (m *Master) mapTaskComplete(f string) error {
	if err := m.eraseInMapFileByName(f); err != nil {
		return err
	}
	m.pushDoneMapFile(f)
	return nil
}

func (m *Master) reduceTaskComplete(taskNo int) error {
	if err := m.eraseInReduceTaskByNo(taskNo); err != nil {
		return err
	}
	m.pushDoneReduceTask(taskNo)
	return nil
}

func (m *Master) WorkResult(req *MapData, resp *MapData) error {
	state := m.getState()
	succ := req.GetString("success")
	outputFile := req.GetStringSlice("outputFile")
	fmt.Println("master get workResult: ", succ)
	if succ == MapTaskDone {
		mapfile := req.GetString("inputFile")
		// mapNo := req.GetInt("inputFileNo")
		fmt.Printf("inputfile: %v outputfile: %v\n", mapfile, outputFile)
		if err := m.mapTaskComplete(mapfile); err != nil {
			log.Println(err)
		}
	} else if succ == MapTaskFail {
		//TODO
	} else if succ == ReduceTaskDone {
		taskNo := req.GetInt("inputFile")
		outputFile := req.GetString("outputFile")
		fmt.Printf("inputfile: %v outputfile: %v\n", taskNo, outputFile)
		if err := m.reduceTaskComplete(taskNo); err != nil {
			log.Println(err)
		}
		if m.getState() == ReduceDone {
			log.Printf("all reduce tasks done. master exit.\n")
			os.Exit(0)
		}
	} else if succ == ReduceTaskFail {
		//TODO
	} else {
		fmt.Println("not accepted success code.", state, (*req)["inputFile"])
	}
	return nil
}

func (m *Master) pushReduceTask(taskNo int) {
	m.reduceTasks = append(m.reduceTasks, taskNo)
	m.mReduceTaskState[taskNo] = Idle
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
func (m *Master) popReduceTask() int {
	if len(m.reduceTasks) == 0 {
		return -1
	}
	ret := m.reduceTasks[0]
	m.reduceTasks = m.reduceTasks[1:]
	return ret
}
func (m *Master) pushInReduceTask(no int) {
	m.inReduceTasks = append(m.inReduceTasks, no)
	m.mReduceTaskState[no] = Using
}

//TODO
func (m *Master) getReduceTask() (MapData, bool) {
	ret := MapData{"nMap": m.nMap}
	if m.isReduceAllDone() {
		return MapData{}, false
	}
	reduceTask := m.popReduceTask()
	if reduceTask == -1 {
		return MapData{}, false
	}

	ret["inputFile"] = reduceTask

	m.pushInReduceTask(reduceTask)
	return ret, true
}

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

func (m *Master) getMapTaskResp() MapData {
	ret, ok := m.getMapTask()
	if ok {
		ret["success"] = GetMapTaskSuccess
	} else {
		ret["success"] = NoMapTask
	}
	return ret
}
func (m *Master) getReduceTaskResp() MapData {
	ret, ok := m.getReduceTask()
	if ok {
		ret["success"] = GetReduceTaskSuccess
	} else {
		ret["success"] = NoReduceTask
	}
	return ret
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
		mp = m.getMapTaskResp()

	} else if state == InReduce {

		//TODO
		mp = m.getReduceTaskResp()
		log.Printf("Master reach inReduce. mpjson:%v\n", JsonString(mp))
	} else {
		mp["success"] = AllDone
	}
	*resp = mp
	log.Println("getWorkerTask resp: ", *resp)

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
