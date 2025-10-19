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
	availableMapTasks map[string]bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	i := 0
	for k, v := range c.availableMapTasks {
		if v {
			reply.Filename = k
			reply.TaskType = Map
			reply.NReduce = c.nReduce
			reply.MapNumber = i
			c.availableMapTasks[k] = false
			log.Printf("Assigned task nr %v, %v to worker\n", i, k)
			return nil
		}
		i++
	}

	// TODO: check for reduce tasks

	log.Println("no available tasks")
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.availableMapTasks = make(map[string]bool)
	for _, f := range files {
		c.availableMapTasks[f] = true
	}
	c.nReduce = nReduce

	log.Println("Coordinator starting...")

	c.server()
	return &c
}
