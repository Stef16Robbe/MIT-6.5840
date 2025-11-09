package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus struct {
	assigned  bool
	completed bool
	startTime time.Time
}

type Coordinator struct {
	mu                sync.Mutex
	nReduce           int
	nMap              int
	availableMapTasks map[string]*TaskStatus // filename -> status
	mapTaskNumbers    map[string]int         // filename -> map task number
	reduceTasks       []*TaskStatus          // indexed by reduce task number
	mapPhaseComplete  bool
}

// Your code here -- RPC handlers for the worker to call.

func (c *Coordinator) GetTask(args *TaskArgs, reply *TaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// Check for timed-out tasks and reassign them
	c.checkTimeouts()

	// First, assign map tasks
	if !c.mapPhaseComplete {
		for filename, status := range c.availableMapTasks {
			if !status.assigned && !status.completed {
				reply.Filename = filename
				reply.TaskType = Map
				reply.NReduce = c.nReduce
				reply.MapNumber = c.mapTaskNumbers[filename]
				status.assigned = true
				status.startTime = time.Now()
				log.Printf("Assigned map task nr %v, %v to worker\n", reply.MapNumber, filename)
				return nil
			}
		}

		// Check if all map tasks are complete
		allMapsDone := true
		for _, status := range c.availableMapTasks {
			if !status.completed {
				allMapsDone = false
				break
			}
		}
		c.mapPhaseComplete = allMapsDone

		if !allMapsDone {
			// Map phase not done, but no tasks available right now
			log.Println("No map tasks available, waiting for in-progress tasks")
			return nil
		}
	}

	// Map phase complete, assign reduce tasks
	for i := 0; i < c.nReduce; i++ {
		if !c.reduceTasks[i].assigned && !c.reduceTasks[i].completed {
			reply.Filename = "" // Not needed for reduce
			reply.TaskType = Reduce
			reply.NReduce = c.nReduce
			reply.MapNumber = i // This is the reduce task number
			c.reduceTasks[i].assigned = true
			c.reduceTasks[i].startTime = time.Now()
			log.Printf("Assigned reduce task %v to worker\n", i)
			return nil
		}
	}

	// Check if all work is done
	allReducesDone := true
	for _, status := range c.reduceTasks {
		if !status.completed {
			allReducesDone = false
			break
		}
	}

	if allReducesDone {
		reply.TaskType = Done
		log.Println("All tasks complete")
	} else {
		log.Println("No tasks available, waiting for in-progress tasks")
	}

	return nil
}

func (c *Coordinator) checkTimeouts() {
	timeout := 10 * time.Second

	// Check map tasks
	for _, status := range c.availableMapTasks {
		if status.assigned && !status.completed {
			if time.Since(status.startTime) > timeout {
				log.Printf("Map task timed out, reassigning")
				status.assigned = false
			}
		}
	}

	// Check reduce tasks
	for i, status := range c.reduceTasks {
		if status.assigned && !status.completed {
			if time.Since(status.startTime) > timeout {
				log.Printf("Reduce task %v timed out, reassigning", i)
				status.assigned = false
			}
		}
	}
}

func (c *Coordinator) FinishTask(args *FinishedTaskArgs, reply *FinishedTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	log.Printf("Task type %v (number %v) finished", args.TaskType, args.TaskNumber)

	switch args.TaskType {
	case Map:
		if status, exists := c.availableMapTasks[args.Filename]; exists {
			status.completed = true
			status.assigned = true
		}
	case Reduce:
		// Now we know exactly which reduce task finished
		if args.TaskNumber >= 0 && args.TaskNumber < c.nReduce {
			c.reduceTasks[args.TaskNumber].completed = true
		}
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
	c.mu.Lock()
	defer c.mu.Unlock()

	for _, status := range c.availableMapTasks {
		if !status.completed {
			return false
		}
	}

	for _, status := range c.reduceTasks {
		if !status.completed {
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

	c.nReduce = nReduce
	c.nMap = len(files)
	c.availableMapTasks = make(map[string]*TaskStatus)
	c.mapTaskNumbers = make(map[string]int)
	c.reduceTasks = make([]*TaskStatus, nReduce)

	for i, f := range files {
		c.availableMapTasks[f] = &TaskStatus{}
		c.mapTaskNumbers[f] = i
	}

	for i := 0; i < nReduce; i++ {
		c.reduceTasks[i] = &TaskStatus{}
	}

	log.Println("Coordinator starting...")

	c.server()
	return &c
}
