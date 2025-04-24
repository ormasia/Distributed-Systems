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

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

type TaskType int

const (
	MapTask TaskType = iota
	ReduceTask
	Wait
	Complete
)

type WorkerArgs struct {
	TaskType   TaskType // MapTask 或 ReduceTask
	TaskNumber int      // 对应的任务编号
}

type WorkerReply struct {
	NMap             int // Reduce 任务需要知道 Map 的数量
	NReduce          int // Map 任务需要知道 Reduce 的数量
	Type             TaskType
	FileName         string // 仅 MapTask 有用
	MapTaskNumber    int    //任务编号 两个只有一个有用
	ReduceTaskNumber int
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
