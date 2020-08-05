package mr

import (
	"encoding/json"
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type Work struct {
	mapf    func(string, string) []KeyValue
	reducef func(string, []string) string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func (m *Work) work() {

	for {
		resp, err := rpcCall("Master.GetWorkerTask", MapData{})
		if err != nil {
			fmt.Println(err, "worker exit.")
			return
		}
		success := resp.GetString("success")
		if success == AllDone {
			fmt.Println("worker get alldone. exit.")
			break
		} else if success == NoMapTask || success == NoReduceTask {
			time.Sleep(time.Duration(1) * time.Second)
			continue
		} else if success == GetMapTaskSuccess {
			postData, err := m.handleMap(resp)
			if err != nil {
				fmt.Println(err)
			}
			fmt.Printf("MapTask postData:\n%v\nend.\n", JsonString(postData))

			resp, err := rpcCall("Master.WorkResult", postData)
			if err != nil {
				fmt.Println(err, resp)
			}

		} else if success == GetReduceTaskSuccess {
			fmt.Printf("get reduce task succ. get json:\n%v\n", JsonString(resp))

			postData, err := m.handleReduce(resp)
			if err != nil {
				fmt.Println(err)
			}
			resp, err := rpcCall("Master.WorkResult", postData)
			if err != nil {
				fmt.Println(err, resp)
			}
		} else {
			fmt.Println("not recognize this success result. ", success)
			time.Sleep(time.Duration(1) * time.Second)
		}

	}

}

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {

	worker := Work{mapf, reducef}
	worker.work()

}

//读文件，调mapf，将kv分区排序写入一个文件，返回文件路径
func (m *Work) handleMap(mp MapData) (MapData, error) {
	log.Printf("map Task:\n%v\n", mp)

	filename := mp.GetString("inputFile")
	fileNo := mp.GetInt("inputFileNo")
	nReduce := mp.GetInt("nReduce")
	taskID := mp.GetInt("taskID")
	log.Println("map filename: ", filename)

	file, err := os.Open(filename)
	if err != nil {
		log.Printf("cannot open %v", filename)
		return MapData{"success": MapTaskFail, "inputFile": filename, "inputFileNo": fileNo}, nil
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Printf("cannot read %v", filename)
		return MapData{"success": MapTaskFail, "inputFile": filename, "inputFileNo": fileNo}, nil
	}
	file.Close()

	kvlist := m.mapf(filename, string(content))
	fmt.Printf("mapf call done. len(kv): %d\n", len(kvlist))

	//分区排序写入文件
	outputFileList, err := m.outputMapKVlist(kvlist, filename, fileNo, nReduce)
	if err != nil {
		log.Println(err)
		return MapData{"success": MapTaskFail, "inputFile": filename, "inputFileNo": fileNo}, nil
	}

	return MapData{
		"success":     MapTaskDone,
		"inputFile":   filename,
		"inputFileNo": fileNo,
		"outputFile":  outputFileList,
		"taskID":      taskID,
	}, nil

}

func makeStringSlice(kvs []KeyValue) []string {
	ret := []string{}
	for _, kv := range kvs {
		ret = append(ret, kv.Value)
	}
	return ret
}

func (m *Work) handleReduce(mp MapData) (MapData, error) {
	//{"inputFile":0,"nMap":1,"success":"GetReduceTaskSuccess"}
	taskNo := mp.GetInt("inputFile")
	nMap := mp.GetInt("nMap")
	taskID := mp.GetInt("taskID")
	//组合出有序kvlist
	kvList, err := m.inputReduceKVlist(taskNo, nMap)
	if err != nil {
		log.Println(err)
		return MapData{"success": ReduceTaskDone, "inputFile": taskNo}, nil
	}

	outputList := []KeyValue{}
	idx := 0
	for i, kv := range kvList {
		if kv.Key != kvList[idx].Key {
			value := m.reducef(kvList[idx].Key, makeStringSlice(kvList[idx:i]))
			outputList = append(outputList, KeyValue{kvList[idx].Key, value})
			idx = i
		}
		if i == len(kvList)-1 {
			value := m.reducef(kvList[idx].Key, makeStringSlice(kvList[idx:i+1]))
			outputList = append(outputList, KeyValue{kvList[idx].Key, value})
		}
	}
	outputFile, err := m.outputReduceKVlist(outputList, taskNo)
	if err != nil {
		log.Printf("cannot output kvlist to file. filename:%v ,err %v.\n", outputFile, err)
		return MapData{"success": ReduceTaskDone, "inputFile": taskNo}, nil
	}
	return MapData{
		"success":    ReduceTaskDone,
		"inputFile":  taskNo,
		"outputFile": outputFile,
		"taskID":     taskID,
	}, nil
}

func (m *Work) outputReduceKVlist(kvs []KeyValue, reduceNo int) (string, error) {
	filename := fmt.Sprintf("mr-out-%d", reduceNo)
	f, err := os.Create(filename)
	if err != nil {
		log.Printf("cannot open file %v . err: %v\n", filename, err)
		return "", err
	}
	defer f.Close()
	for _, kv := range kvs {
		_, err := fmt.Fprintf(f, "%v %v\n", kv.Key, kv.Value)
		if err != nil {
			log.Printf("cannot write to file %v . err: %v\n", filename, err)
			return "", err
		}
	}
	return filename, nil
}

func (m *Work) inputReduceKVlist(taskNo, nMap int) ([]KeyValue, error) {
	ret := []KeyValue{}
	for i := 0; i < nMap; i++ {
		filename := fmt.Sprintf("mr-%d-%d", i, taskNo)
		f, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open file %v.err: %v\n", filename, err)
			return []KeyValue{}, err
		}
		tmp := []KeyValue{}
		dec := json.NewDecoder(f)
		if err := dec.Decode(&tmp); err != nil {
			log.Printf("cannot read file %v.err: %v\n", filename, err)
			return []KeyValue{}, err
		}
		ret = append(ret, tmp...)
		f.Close()
	}
	sort.Sort(ByKey(ret))
	return ret, nil

}

func calBucket(key string, m int) int {
	return ihash(key) % m
}

//分区排序写文件
func (m *Work) outputMapKVlist(kvlist []KeyValue, filename string, fileNo int, nReduce int) ([]string, error) {
	kvs := make([]([]KeyValue), nReduce)
	filepaths := []string{}
	for _, kv := range kvlist {
		bucket := calBucket(kv.Key, nReduce)
		kvs[bucket] = append(kvs[bucket], kv)
	}
	for i, _ := range kvs {
		sort.Sort((ByKey)(kvs[i]))
		outfileName := fmt.Sprintf("mr-%d-%d", fileNo, i)
		filepaths = append(filepaths, outfileName)
		of, err := os.Create(outfileName)
		if err != nil {
			log.Println(err)
			return []string{}, err
		}
		enc := json.NewEncoder(of)
		if err := enc.Encode(&kvs[i]); err != nil {
			log.Println(err)
			return []string{}, err
		}
		of.Close()
	}
	return filepaths, nil
}

func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Println("dialing:", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println("call error: ", err)
	return false
}

func rpcCall(f string, req MapData) (MapData, error) {
	resp := MapData{}
	succ := call(f, &req, &resp)
	if succ {
		return resp, nil
	}
	s := fmt.Sprintf("call func %v failed.", f)
	return resp, errors.New(s)
}
