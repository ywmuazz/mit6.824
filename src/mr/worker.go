package mr

import (
	"errors"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
)

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	resp, err := callGetMapFilename()
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println("filename: ", resp.Filename)

	kvlist, err := handleMap(resp.Filename, mapf)
	if err != nil {
		fmt.Println(err)
	}
	fmt.Println(len(kvlist))

	respw, err := callWorkerDone()
	if err != nil {
		fmt.Println("return code: ", respw.Success)
	}
}

func callGetMapFilename() (*MapFilenameResp, error) {
	req := MapFilenameReq{"A"}
	resp := MapFilenameResp{}
	succ := call("Master.GetMapFilename", &req, &resp)
	if succ {
		return &resp, nil
	}
	return nil, errors.New("call func failed")
	// fmt.Println("filename: ", resp.Filename)
}
func callWorkerDone() (*WorkerDoneResp, error) {
	req := WorkerDoneReq{}
	resp := WorkerDoneResp{}
	succ := call("Master.WorkerDone", &req, &resp)
	if succ {
		return &resp, nil
	}
	return nil, errors.New("call func failed")
}

func handleMap(filename string, mapf func(string, string) []KeyValue) ([]KeyValue, error) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalf("cannot open %v", filename)
	}

	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", filename)
	}
	file.Close()
	kv := mapf(filename, string(content))
	return kv, nil
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//

// func CallExample() {
// 	// declare an argument structure.
// 	args := ExampleArgs{}
// 	// fill in the argument(s).
// 	args.X = 99
// 	// declare a reply structure.
// 	reply := ExampleReply{}
// 	// send the RPC request, wait for the reply.
// 	call("Master.Example", &args, &reply)
// 	// reply.Y should be 100.
// 	fmt.Printf("reply.Y %v\n", reply.Y)
// }

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
