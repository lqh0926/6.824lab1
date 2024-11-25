package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type TaskStatus struct {
	InProgress bool
	Completed  bool
	StartTime  time.Time
}
type Coordinator struct {
	// Your definitions here.
	MapTasks             []TaskStatus
	ReduceTasks          []TaskStatus
	CompletedMapTasks    int
	CompletedReduceTasks int
	nMap                 int
	nReduce              int

	// 输入和中间文件信息
	InputFiles        []string
	IntermediateFiles map[int][]string // Map任务编号 -> 中间文件

	// 并发控制和超时
	Mutex       sync.Mutex
	TaskTimeout time.Duration // 任务超时时间

	// 任务完成标志
	AllTasksCompleted bool
	DoneChannel       chan bool
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) AssignTask(args *RequestTask, reply *AssignTask) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if !c.allMapTasksCompleted() {
		for i, task := range c.MapTasks {
			if !task.InProgress && !task.Completed {
				c.MapTasks[i].InProgress = true
				c.MapTasks[i].StartTime = time.Now()
				reply.Filename = []string{c.InputFiles[i]}
				reply.WorkId = i
				reply.Worktype = "Map"
				reply.NReduce = c.nReduce
				return nil
			}
		}
	} else if !c.allReduceTasksCompleted() {
		for i, task := range c.ReduceTasks {
			if !task.InProgress && !task.Completed {
				c.ReduceTasks[i].InProgress = true
				c.ReduceTasks[i].StartTime = time.Now()
				reply.Filename = c.IntermediateFiles[i]
				reply.WorkId = i
				reply.Worktype = "Reduce"
				reply.NReduce = c.nReduce
				return nil
			}
		}
	}
	if c.allMapTasksCompleted() && c.allReduceTasksCompleted() {
		c.AllTasksCompleted = true
		reply.Worktype = "Done"
		return nil
	}
	reply.Worktype = "Wait"
	return nil

}

func (c *Coordinator) MarkTaskCompleted(args *TaskCompleteArgs, reply *TaskCompleteReply) error {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	if args.Worktype == "Map" {
		c.MapTasks[args.WorkId].Completed = true
		for _, file := range args.IntermediateFiles {
			id, _ := strconv.Atoi(strings.Split(file, "-")[2])
			c.IntermediateFiles[id] = append(c.IntermediateFiles[id], file)
		}
		c.CompletedMapTasks++
		//fmt.Printf("Map Task %v Completed", args.WorkId)
	} else if args.Worktype == "Reduce" {
		c.ReduceTasks[args.WorkId].Completed = true
		c.CompletedReduceTasks++
	}
	return nil
}
func (c *Coordinator) allMapTasksCompleted() bool {
	return c.CompletedMapTasks == c.nMap
}

func (c *Coordinator) allReduceTasksCompleted() bool {
	return c.CompletedReduceTasks == c.nReduce
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
	for !c.Done() {
		time.Sleep(time.Second)
		c.ReassignTask()
	}
	c.DoneChannel <- true
}

func (c *Coordinator) ReassignTask() {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	for i, task := range c.MapTasks {
		if task.InProgress && time.Since(task.StartTime) > c.TaskTimeout {
			c.MapTasks[i].InProgress = false
		}
	}
	for i, task := range c.ReduceTasks {
		if task.InProgress && time.Since(task.StartTime) > c.TaskTimeout {
			c.ReduceTasks[i].InProgress = false
		}
	}
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.Mutex.Lock()
	defer c.Mutex.Unlock()
	return c.allMapTasksCompleted() && c.allReduceTasksCompleted()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nMap:              len(files),
		nReduce:           nReduce,
		InputFiles:        files,
		IntermediateFiles: make(map[int][]string),
		TaskTimeout:       20 * time.Second,
		DoneChannel:       make(chan bool),
	}
	for i := 0; i < len(files); i++ {
		c.MapTasks = append(c.MapTasks, TaskStatus{})
	}
	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, TaskStatus{})
	}
	// Your code here.

	go c.server()
	return &c
}
