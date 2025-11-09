package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"sort"
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
			ok := reportFinishedTask(Map, task.Filename, task.MapNumber)
			if !ok {
				log.Println("Could not report that map task was finished")
			}
		case Reduce:
			log.Printf("Starting reducing (reduce task #%v)...\n", task.MapNumber)
			perform_reduce(reducef, task)
			ok := reportFinishedTask(Reduce, "", task.MapNumber)
			if !ok {
				log.Println("Could not report that reduce task was finished")
			}
		case Done:
			log.Println("All tasks done, exiting")
			return
		}
	}
}

func perform_reduce(reducef func(string, []string) string, task TaskReply) {
	var kva []KeyValue

	reduceTaskNumber := task.MapNumber // This is actually the reduce task number

	// Read intermediate files from ALL map tasks for this reduce task
	// Files are named mr-{mapTaskNumber}-{reduceTaskNumber}
	mapTaskNumber := 0
	for {
		filename := fmt.Sprintf("mr-%d-%d", mapTaskNumber, reduceTaskNumber)
		file, err := os.Open(filename)
		if err != nil {
			// No more map task files to read
			log.Printf("Finished reading intermediate files (tried %v)\n", filename)
			break
		}

		log.Printf("Reading file %v\n", filename)
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				// EOF or decode error
				break
			}
			kva = append(kva, kv)
		}

		file.Close()
		mapTaskNumber++
	}

	// Sort by key for consistent output
	sort.Slice(kva, func(i, j int) bool {
		return kva[i].Key < kva[j].Key
	})

	// Group values by key
	keyToValues := make(map[string][]string)
	for _, kv := range kva {
		keyToValues[kv.Key] = append(keyToValues[kv.Key], kv.Value)
	}

	// Create output file
	filename := fmt.Sprintf("mr-out-%v", reduceTaskNumber)
	tmpfile := fmt.Sprintf("mr-out-%v-tmp", reduceTaskNumber)
	file, err := os.Create(tmpfile)
	if err != nil {
		log.Fatalf("Failed creating reduce output file %v: %v", tmpfile, err)
	}

	// Get keys in sorted order for deterministic output
	keys := make([]string, 0, len(keyToValues))
	for key := range keyToValues {
		keys = append(keys, key)
	}
	sort.Strings(keys)

	// Call reduce on each key with all its values
	for _, key := range keys {
		output := reducef(key, keyToValues[key])
		fmt.Fprintf(file, "%v %v\n", key, output)
	}

	file.Close()

	// Atomically rename temp file to final output
	os.Rename(tmpfile, filename)

	log.Printf("Reduced to: %v\n", filename)
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
	log.Printf("Mapped file: %v, produced %v key-value pairs", task.Filename, len(kvs))

	// Partition into intermediate files by reduce task number
	intermediate := make(map[int][]KeyValue)
	for _, kv := range kvs {
		reduceNumber := ihash(kv.Key) % task.NReduce
		intermediate[reduceNumber] = append(intermediate[reduceNumber], kv)
	}

	// Write intermediate files atomically using temp files
	for i := 0; i < task.NReduce; i++ {
		filename := fmt.Sprintf("mr-%v-%v", task.MapNumber, i)
		tmpfilename := fmt.Sprintf("mr-%v-%v-tmp", task.MapNumber, i)

		file, err := os.Create(tmpfilename)
		if err != nil {
			log.Fatalf("Failed to create temp file %v: %v", tmpfilename, err)
		}

		enc := json.NewEncoder(file)
		for _, kv := range intermediate[i] {
			err = enc.Encode(&kv)
			if err != nil {
				log.Fatalf("err JSON encoding key %v: %v", kv, err)
			}
		}

		err = file.Close()
		if err != nil {
			log.Fatalf("err closing file %v: %v", tmpfilename, err)
		}

		// Atomically rename temp file to final name
		err = os.Rename(tmpfilename, filename)
		if err != nil {
			log.Fatalf("err renaming file %v to %v: %v", tmpfilename, filename, err)
		}
	}

	log.Printf("Wrote %v intermediate files for map task %v", task.NReduce, task.MapNumber)
}

func requestTask() (TaskReply, bool) {
	reply := TaskReply{}
	args := TaskArgs{}

	ok := call("Coordinator.GetTask", &args, &reply)
	if ok {
		if reply.TaskType == Done {
			log.Printf("Coordinator says all work is done")
			return reply, true
		}
		if reply.Filename == "" && reply.TaskType != Reduce {
			log.Printf("Received 'nil' task, no work available")
			return reply, false
		}
		log.Printf("Received task: type=%v, file=%v, mapNumber=%v\n",
			reply.TaskType, reply.Filename, reply.MapNumber)
		return reply, true
	} else {
		log.Println("(GetTask) RPC Call failed - coordinator likely finished")
		return reply, false
	}
}

func reportFinishedTask(tt TaskType, filename string, taskNumber int) bool {
	args := FinishedTaskArgs{
		TaskType:   tt,
		Filename:   filename,
		TaskNumber: taskNumber,
	}
	reply := FinishedTaskReply{}
	ok := call("Coordinator.FinishTask", &args, &reply)

	if ok {
		log.Printf("Successfully reported finished task: type=%v, number=%v", tt, taskNumber)
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
