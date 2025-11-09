package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	nReduce           int
	availableMapTasks map[string]int
	reduceTasks       map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	// TODO: 10 seconds after giving any of these tasks away,
	// if they have not been reported as finished,
	// we give it to another worker
	for filename, mapNr := range c.availableMapTasks {
		if mapNr != -1 {
			reply.Filename = filename
			reply.TaskType = Map
			reply.NReduce = c.nReduce
			reply.MapNumber = mapNr
			c.availableMapTasks[filename] = -1
			log.Printf("Assigned map task nr %v, %v to worker\n", mapNr, filename)
			return nil
		}
	}

	// TODO assign tasks based on mapnr
	// TODO rename mapnr to tasknr or smth
	for filename, available := range c.reduceTasks {
		if available {
			reply.Filename = filename
			reply.TaskType = Reduce
			c.reduceTasks[filename] = false
			log.Printf("Assigned reduce task %v to worker\n", filename)
			return nil
		}
	}

	log.Println("no available tasks")
	return nil
}

func (c *Coordinator) FinishTask(args *FinishedTaskArgs, reply *TaskReply) error {
	log.Printf("task type %v with file %v finished", args.TaskType, args.Filename)

	switch args.TaskType {
	case Map:
		// Said file has been mapped, it can be reduced
		c.reduceTasks[args.Filename] = true
	case Reduce:
		// TODO: what to do after reduce?
	}
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	err := rpc.Register(c)
	if err != nil {
		log.Fatalf("error registering RPC server: %v", err)
	}
	rpc.HandleHTTP()
	sockname := coordinatorSock()
	_ = os.Remove(sockname)

	// l, e := net.Listen("tcp", ":1234")
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}

	go func() {
		if err := http.Serve(l, nil); err != nil {
			log.Printf("HTTP server error: %v", err)
		}
	}()
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	for _, v := range c.availableMapTasks {
		if v != -1 {
			return false
		}
	}

	for _, v := range c.reduceTasks {
		if v {
			return false
		}
	}

	return true
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.availableMapTasks = make(map[string]int)
	c.reduceTasks = make(map[string]bool)
	for i, f := range files {
		c.availableMapTasks[f] = i
		c.reduceTasks[f] = false
	}
	c.nReduce = nReduce

	log.Println("Coordinator starting...")

	c.server()
	return &c
}
