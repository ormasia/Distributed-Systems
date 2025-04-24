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

// 任务状态
const (
	Idle = iota
	InProgress
	Done
)

type Coordinator struct {
	// Your definitions here.
	files        []string   // 输入文件名列表，用于生成 Map 任务
	nMap         int        // Map 任务总数
	nReduce      int        // Reduce 任务总数
	nMapDone     int        // 已完成的 Map 任务数
	nReduceDone  int        // 已完成的 Reduce 任务数
	mapStatus    []int      // 每个 Map 任务的状态：0 idle，1 in-progress，2 done
	reduceStatus []int      // 每个 Reduce 任务的状态：同上
	mu           sync.Mutex // 协调器的互斥锁，用于保护共享状态
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) AssignTask(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	//先完成map再完成reduce
	if c.nMapDone < c.nMap {
		c.scheduleMapTask(reply)
	} else if c.nReduceDone < c.nReduce {
		c.scheduleReduceTask(reply)
	} else {
		reply.Type = Complete
	}
	return nil
}

func (c *Coordinator) scheduleMapTask(reply *WorkerReply) {
	task := -1
	for i := range c.nMap { //多一个保证，保证获取到idle任务
		if c.mapStatus[i] == Idle {
			task = i
			break
		}
	}
	if task == -1 { //没有任务，改成Wait
		reply.Type = Wait
	} else {
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = MapTask
		reply.FileName = c.files[task]
		reply.MapTaskNumber = task
		//设置任务状态为"进行中"
		c.mapStatus[task] = InProgress

		go func(num int) {
			time.Sleep(10 * time.Second)
			c.mu.Lock() //上锁消耗有点大？
			defer c.mu.Unlock()

			// 如果一直没有完成
			if c.mapStatus[num] == InProgress {
				c.mapStatus[num] = Idle
			}
		}(task)
	}
}

func (c *Coordinator) scheduleReduceTask(reply *WorkerReply) {
	task := -1
	for i := range c.nReduce { //多一个保证，保证获取到idle任务
		if c.reduceStatus[i] == Idle {
			task = i
			break
		}
	}
	if task == -1 { //没有任务，改成Wait
		reply.Type = Wait
	} else {
		reply.NMap = c.nMap
		reply.NReduce = c.nReduce
		reply.Type = ReduceTask
		reply.ReduceTaskNumber = task
		//设置任务状态为"进行中"
		c.reduceStatus[task] = InProgress

		go func(num int) {
			time.Sleep(10 * time.Second)
			c.mu.Lock() //用 channel 替代 Sleep + Lock 的 goroutine
			defer c.mu.Unlock()

			// 如果一直没有完成
			if c.reduceStatus[num] == 1 {
				c.reduceStatus[num] = 0
			}
		}(task)
	}
}

func (c *Coordinator) MapTaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	num := args.TaskNumber
	if c.mapStatus[num] != Done {
		c.mapStatus[num] = Done
		c.nMapDone++
	}
	return nil
}

func (c *Coordinator) ReduceTaskFinish(args *WorkerArgs, reply *WorkerReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	num := args.TaskNumber
	if c.reduceStatus[num] != Done {
		c.reduceStatus[num] = Done
		c.nReduceDone++
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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

	if c.nMapDone == c.nMap && c.nReduceDone == c.nReduce {
		ret = true
	}

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.files = files
	c.nReduce = nReduce
	c.nMap = len(files)
	c.nReduceDone = 0
	c.nMapDone = 0
	c.mapStatus = make([]int, c.nMap)
	c.reduceStatus = make([]int, c.nReduce)

	c.server()
	return &c
}
