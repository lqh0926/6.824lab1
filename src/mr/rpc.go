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

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type RequestTask struct {
}

type AssignTask struct {
	Filename []string
	NReduce  int
	WorkId   int
	Worktype string
}

type TaskCompleteArgs struct {
	WorkId            int
	Worktype          string
	IntermediateFiles map[int][]string
}
type TaskCompleteReply struct {
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
