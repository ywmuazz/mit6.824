package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"log"
	"net/rpc"
	"time"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

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
	filename := mp.GetString("inputFile")
	fmt.Println("map filename: ", filename)
	// file, err := os.Open(filename)
	// if err != nil {
	// 	log.Fatalf("cannot open %v", filename)
	// }

	// content, err := ioutil.ReadAll(file)
	// if err != nil {
	// 	log.Fatalf("cannot read %v", filename)
	// }
	// file.Close()
	// kv := m.mapf(filename, string(content))
	// fmt.Printf("mapf call done. len(kv): %d\n", len(kv))
	//分区排序写入文件
	return MapData{"success": MapTaskDone, "inputFile": filename, "outputFile": filename + "done"}, nil
}

func (m *Work) handleReduce(mp MapData) (MapData, error) {
	filename := mp.GetString("inputFile")
	fmt.Println("reduce filename: ", filename)
	return MapData{"success": ReduceTaskDone, "inputFile": filename, "outputFile": filename + "done"}, nil
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
