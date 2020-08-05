package mr

import (
	"errors"
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type MasterState int
type SplitStat int
type TaskType int
type TaskState int

//未防止中途任务失败，所以没有任务不代表所有任务已完成，需要轮询继续等待alldone

//若定时fail和result同时到达，若fail抢锁成功，则认为taskfail，而若result先抢到锁，则fail检测到状态已改变之后就停止
type TaskRec struct {
	taskID   int
	taskType TaskType
	mapFile  string
	taskNo   int
	state    TaskState
	timer    *time.Timer
	resCome  chan struct{}
}

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

	taskRecs []*TaskRec

	start bool
	done  bool

	chanForExit chan struct{}

	sync.Mutex
}

func (m *Master) Exit() {
	<-m.chanForExit
	fmt.Println("master process exit.")
	time.Sleep(time.Duration(500) * time.Millisecond)
	os.Exit(0)
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
	go m.Exit()
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
	m := Master{
		filenames:        files,
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
		chanForExit:      make(chan struct{}),
		taskRecs:         []*TaskRec{},
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

func (m *Master) getNumAllMapTask() int  { return len(m.filenames) }
func (m *Master) getNumMapFiles() int    { return len(m.mapFiles) }
func (m *Master) getNumInMapTask() int   { return len(m.inMapFiles) }
func (m *Master) getNumDoneMapTask() int { return len(m.doneMapFiles) }
func (m *Master) isMapAllDone() bool     { return m.getNumDoneMapTask() == m.getNumAllMapTask() }

func (m *Master) getNumAllReduceTask() int  { return m.nReduce }
func (m *Master) getNumReduceFiles() int    { return len(m.reduceTasks) }
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

func (m *Master) WorkResult(req *MapData, resp *MapData) error {
	m.Lock()
	defer m.Unlock()

	state := m.getState()
	succ := req.GetString("success")
	outputFile := req.GetStringSlice("outputFile")
	taskID := req.GetInt("taskID")
	fmt.Println("master get workResult: ", succ)
	//先把定时器关了
	m.taskRecs[taskID].resCome <- struct{}{}
	if succ == MapTaskDone {
		mapfile := req.GetString("inputFile")
		fmt.Printf("inputfile: %v outputfile: %v\n", mapfile, outputFile)
		if err := m.mapTaskComplete(taskID, mapfile); err != nil {
			log.Println(err)
		}
	} else if succ == MapTaskFail {
		mapfile := req.GetString("inputFile")
		fmt.Printf("inputfile: %v outputfile: %v\n", mapfile, outputFile)
		if err := m.mapTaskFail(taskID, mapfile); err != nil {
			log.Println(err)
		}
	} else if succ == ReduceTaskDone {
		taskNo := req.GetInt("inputFile")
		outputFile := req.GetString("outputFile")
		fmt.Printf("inputfile: %v outputfile: %v\n", taskNo, outputFile)
		if err := m.reduceTaskComplete(taskID, taskNo); err != nil {
			log.Println(err)
		}
		if m.getState() == ReduceDone {
			log.Printf("all reduce tasks done. master will exit soon.\n")
			close(m.chanForExit)
		}
	} else if succ == ReduceTaskFail {
		taskNo := req.GetInt("inputFile")
		fmt.Println("reduce task fail, taskNo: ", taskNo)
		if err := m.reduceTaskFail(taskID, taskNo); err != nil {
			log.Println(err)
		}
	} else {
		fmt.Println("not accepted success code.", state, (*req)["inputFile"])
	}
	return nil
}

func (m *Master) GetWorkerTask(req *MapData, resp *MapData) error {
	m.Lock()
	defer m.Unlock()
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

		mp = m.getReduceTaskResp()
		log.Printf("Master reach inReduce. mpjson:%v\n", JsonString(mp))
	} else {
		mp["success"] = AllDone
	}

	*resp = mp
	log.Println("getWorkerTask resp: ", *resp)

	return nil
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

func (m *Master) mapTaskComplete(taskID int, f string) error {
	if m.taskRecs[taskID].state != Waiting {
		return errors.New("state change. cannot sol mapTaskComplete.")
	}
	if err := m.eraseInMapFileByName(f); err != nil {
		return err
	}
	m.pushDoneMapFile(f)
	m.taskRecs[taskID].state = Success
	return nil
}
func (m *Master) mapTaskFail(taskID int, f string) error {
	if m.taskRecs[taskID].state != Waiting {
		return errors.New("state change. cannot sol mapTaskFail.")
	}
	if err := m.eraseInMapFileByName(f); err != nil {
		return err
	}
	m.pushMapFile(f)
	m.taskRecs[taskID].state = Fail
	return nil
}

func (m *Master) reduceTaskComplete(taskID, taskNo int) error {
	if m.taskRecs[taskID].state != Waiting {
		return errors.New("state change. cannot sol reduceTaskComplete.")
	}
	if err := m.eraseInReduceTaskByNo(taskNo); err != nil {
		return err
	}
	m.pushDoneReduceTask(taskNo)
	m.taskRecs[taskID].state = Success
	return nil
}

func (m *Master) reduceTaskFail(taskID, taskNo int) error {
	if m.taskRecs[taskID].state != Waiting {
		return errors.New("state change. cannot sol reduceTaskFail.")
	}
	if err := m.eraseInReduceTaskByNo(taskNo); err != nil {
		return err
	}
	m.pushReduceTask(taskNo)
	m.taskRecs[taskID].state = Fail
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

func (m *Master) mapTaskExpired(taskID int, f string) {
	m.Lock()
	defer m.Unlock()
	if err := m.mapTaskFail(taskID, f); err != nil {
		//不影响继续运行的err
		log.Println(err)
	}
}
func (m *Master) reduceTaskExpired(taskID, taskNo int) {
	m.Lock()
	defer m.Unlock()
	if err := m.reduceTaskFail(taskID, taskNo); err != nil {
		//不影响继续运行的err
		log.Println(err)
	}
}

func (m *Master) getNumTaskRec() int {
	return len(m.taskRecs)
}

func (m *Master) addMapTaskRec(f string) int {
	t := &TaskRec{
		taskID:   m.getNumTaskRec(),
		taskType: MapType,
		mapFile:  f,
		taskNo:   -1,
		state:    Waiting,
		timer:    time.NewTimer(time.Duration(expiredTime) * time.Second),
		resCome:  make(chan struct{}, 1),
	}
	m.taskRecs = append(m.taskRecs, t)
	go func(id int) {
		select {
		case <-m.taskRecs[id].timer.C:
			m.mapTaskExpired(id, f)
			break
		case <-m.taskRecs[id].resCome:
			if m.taskRecs[id].timer.Stop() {
				log.Printf("Stop timer success. taskID:%v.\n", t.taskID)
			} else {
				log.Printf("Stop timer fail. taskID:%v.\n", t.taskID)
			}
			break
		}

	}(t.taskID)
	return t.taskID
}

func (m *Master) addReduceTaskRec(no int) int {
	t := &TaskRec{
		taskID:   m.getNumTaskRec(),
		taskType: ReduceType,
		mapFile:  "",
		taskNo:   no,
		state:    Waiting,
		timer:    time.NewTimer(time.Duration(expiredTime) * time.Second),
		resCome:  make(chan struct{}, 1),
	}
	m.taskRecs = append(m.taskRecs, t)
	go func(id int) {
		select {
		case <-m.taskRecs[id].timer.C:
			m.reduceTaskExpired(id, no)
			break
		case <-m.taskRecs[id].resCome:
			if m.taskRecs[id].timer.Stop() {
				log.Printf("Stop timer success. taskID:%v.\n", t.taskID)
			} else {
				log.Printf("Stop timer fail. taskID:%v.\n", t.taskID)
			}
			break
		}

	}(t.taskID)
	return t.taskID
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

	taskID := m.addReduceTaskRec(reduceTask)
	ret["taskID"] = taskID

	return ret, true
}

func (m *Master) getMapTask() (MapData, bool) {
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

	taskID := m.addMapTaskRec(mapFile)
	ret["taskID"] = taskID

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
