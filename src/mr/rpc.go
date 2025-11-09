package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

// Add your RPC definitions here.
type TaskType int16

const (
	Map TaskType = iota
	Reduce
	Done
)

type TaskArgs struct{}

type TaskReply struct {
	Filename  string
	TaskType  TaskType
	NReduce   int
	MapNumber int // For Map tasks: the map task number. For Reduce tasks: the reduce task number
}

type FinishedTaskArgs struct {
	TaskType   TaskType
	Filename   string // Only used for Map tasks
	TaskNumber int    // For Map tasks: map number. For Reduce tasks: reduce number
}

type FinishedTaskReply struct {
	Die bool
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
