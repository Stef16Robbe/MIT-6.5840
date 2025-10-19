package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	task := CallRequestTask()
	log.Printf("Worker %v starting...\n", task.MapNumber)

	//
	// read each input file,
	// pass it to Map,
	// accumulate the intermediate Map output.
	//
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()

	if task.TaskType == Map {
		perform_mapping(mapf, task, content)
	} else {
		// TODO: reduce
	}

}

func perform_mapping(mapf func(string, string) []KeyValue, task TaskReply, content []byte) {
	kvs := mapf(task.Filename, string(content))
	log.Printf("mapped file: %v", task.Filename)
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kvs {
		reduceNumber := ihash(kv.Key) % task.NReduce
		intermediate[reduceNumber] = append(intermediate[reduceNumber], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%d-%d", task.MapNumber, i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			enc.Encode(&kv)
		}
		file.Close()
	}
}

func CallRequestTask() TaskReply {
	reply := TaskReply{}
	args := TaskArgs{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if !ok {
		log.Println("RPC Call failed")
		os.Exit(1)
	}

	log.Printf("received task: %v\n", reply)
	return reply
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
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
