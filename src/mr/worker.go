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

func JsonString(x interface{}) string {
	b, err := json.Marshal(x)
	if err != nil {
		return ""
	}
	return string(b)
}

func (m *Work) work() {

	for {
		resp, err := rpcCall("Master.GetWorkerTask", MapData{})
		if err != nil {
			fmt.Println(err)
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
			fmt.Printf("MapTask postData:\n%v\nend.", JsonString(postData))
			os.Exit(0)

			resp, err := rpcCall("Master.WorkResult", postData)
			if err != nil {
				fmt.Println(err, resp)
			}
		} else if success == GetReduceTaskSuccess {
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
	log.Println("map filename: ", filename)
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kvlist := m.mapf(filename, string(content))
	fmt.Printf("mapf call done. len(kv): %d\n", len(kvlist))
	outputFileList, err := outputKVlist(kvlist, filename, fileNo, nReduce)
	if err != nil {
		log.Println(err)
	}
	//分区排序写入文件
	return MapData{"success": MapTaskDone, "inputFile": filename, "outputFile": outputFileList}, nil
}

func (m *Work) handleReduce(mp MapData) (MapData, error) {
	filename := mp.GetString("inputFile")
	fmt.Println("reduce filename: ", filename)
	return MapData{"success": ReduceTaskDone, "inputFile": filename, "outputFile": filename + "done"}, nil
}

func calBucket(key string, m int) int {
	return ihash(key) % m
}

//分区排序写文件
func outputKVlist(kvlist []KeyValue, filename string, fileNo int, nReduce int) ([]string, error) {
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
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
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

func (m MapData) GetString(key string) string {
	value, exist := m[key]
	if !exist {
		return ""
	}
	str, ok := value.(string)
	if !ok {
		return ""
	}
	return str
}

func (m MapData) GetMapData(key string) MapData {
	value, exist := m[key]
	if !exist {
		return make(map[string]interface{}, 0)
	}
	str, ok := value.(map[string]interface{})
	if !ok {
		return make(map[string]interface{}, 0)
	}
	return str
}

func (m MapData) GetInt(key string) int {
	value, exist := m[key]
	if !exist {
		return 0
	}
	ret, ok := value.(int)
	if !ok {
		return 0
	}
	return ret
}
