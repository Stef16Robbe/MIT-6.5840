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
	for {
		task, ok := requestTask()

		if !ok {
			// RPC failed, coordinator probably done
			log.Println("Cannot contact coordinator or received no task, exiting")
			break
		}

		switch task.TaskType {
		case Map:
			log.Printf("Starting mapping (#%v)...\n", task.MapNumber)
			perform_mapping(mapf, task)
			ok := reportFinishedTask(Map, task.Filename)
			if !ok {
				log.Fatal("Could not report that task was finished")
			}
		case Reduce:
			log.Printf("Starting reducing (%v)...\n", task.MapNumber)
			perform_reduce(reducef, task)
			ok := reportFinishedTask(Reduce, task.Filename)
			if !ok {
				log.Fatal("Could not report that task was finished")
			}
		case Done:
			log.Println("Done")
		}
	}
}

func perform_reduce(reducef func(string, []string) string, task TaskReply) {
	var kva []KeyValue

	reduceTask := 0
	for {
		filename := fmt.Sprintf("mr-%d-%d", task.MapNumber, reduceTask)
		log.Printf("Reading file %v\n", filename)
		file, err := os.Open(filename)
		if err != nil {
			log.Printf("cannot open %v", filename)
			break
		}

		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// it's ok to err on EOF
				// since go errors kinda suck we will just continue on any errors
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
		reduceTask++
	}

	// Group values by key
	keyToValues := make(map[string][]string)
	for _, kv := range kva {
		keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)
	}

	filename := fmt.Sprintf("mr-out-%v", task.MapNumber)
	file, err := os.Create(filename)
	if err != nil {
		log.Fatalf("Failed creating reduce output file %v: %v", filename, err)
	}

	// Now call reduce on each key with all its values
	for key, values := range keyToValues {
		output := reducef(key, values)
		fmt.Fprintf(file, "%v %v\n", key, output)
	}
	file.Close()
}

func perform_mapping(mapf func(string, string) []KeyValue, task TaskReply) {
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := io.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}

	err = file.Close()
	if err != nil {
		log.Fatalf("err closing file %v", task.Filename)
	}

	kvs := mapf(task.Filename, string(content))
	log.Printf("mapped file: %v", task.Filename)
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kvs {
		reduceNumber := ihash(kv.Key) % task.NReduce
		intermediate[reduceNumber] = append(intermediate[reduceNumber], kv)
	}

	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", task.MapNumber, i)
		file, _ := os.Create(filename)
		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("err JSON encoding key %v", kv)
			}
		}
		err = file.Close()
		if err != nil {
			log.Fatalf("err closing file %v", task.Filename)
		}
	}
}

func requestTask() (TaskReply, bool) {
	reply := TaskReply{}
	args := TaskArgs{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		if reply.Filename == "" {
			log.Printf("received 'nil' task, looks like we're done")
			return reply, false
		}
		log.Printf("received task: %v\n", reply)
		return reply, true
	} else {
		log.Println("(GetTask) RPC Call failed - coordinator likely finished")
		return reply, false
	}
}

func reportFinishedTask(tt TaskType, filename string) bool {
	args := FinishedTaskArgs{TaskType: tt, Filename: filename}
	ok := call("Coordinator.FinishTask", &args, nil)

	if ok {
		log.Printf("succesfully reported finished task")
		return true
	} else {
		log.Println("(FinishTask) RPC Call failed - coordinator likely finished")
		return false
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args any, reply any) bool {
	sockname := coordinatorSock()

	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Printf("error dialing: %v", err)
		return false
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	log.Printf("error calling coordinator: %v\n", err)
	return false
}
