package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"sync/atomic"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	State               int // 0 map, 1 reduce, 2 none
	MapTaskChan         chan Task
	ReduceTaskChan      chan Task
	CurMapTask          int32
	CurReduceTask       int32
	MapTaskNum          int32
	ReduceTaskNum       int32
	MapTaskFinishNum    int32
	ReduceTaskFinishNum int32
	MapFinishMap        sync.Map
	ReduceFinishMap     sync.Map
	mu                  sync.RWMutex
	files               []string
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	switch c.State {
	case MapTaskType:
		log.Printf("------- prepare to get map task")
		task := <-c.MapTaskChan
		reply.Task = Task{
			FileName:  task.FileName,
			MapTaskNo: task.MapTaskNo,
		}
		c.MapFinishMap.Store(task.MapTaskNo, Finish{StartTime: getTimeStamp()})

		reply.TaskType = MapTaskType
		reply.ReduceTaskNum = c.ReduceTaskNum
		log.Printf(">>>>>>>>>>>>>>>>>>>> send map task: %+v", reply.Task)
		atomic.AddInt32(&c.CurMapTask, 1)
	case ReduceTaskType:
		log.Printf("------- prepare to get redeuce task")
		task := <-c.ReduceTaskChan
		reply.TaskType = ReduceTaskType
		reply.ReduceTaskNum = c.ReduceTaskNum
		reply.MapTaskNum = c.MapTaskNum
		reply.Task = Task{
			ReduceTaskNo: task.ReduceTaskNo,
		}
		c.ReduceFinishMap.Store(task.ReduceTaskNo, Finish{StartTime: getTimeStamp()})
		log.Printf(">>>>>>>>>>>>>>>>>>>>>> send reduce task: %+v", reply.Task)
		atomic.AddInt32(&c.CurReduceTask, 1)
	case NoneTaskType:
		log.Printf("send nont type")
		reply.TaskType = NoneTaskType
	}
	return nil
}

func (c *Coordinator) TaskFinishNotify(args *TaskFinishNotifyArgs, reply *TaskFinishNotifyReply) error {
	// c.mu.RLock()
	// defer c.mu.RUnlock()
	switch args.TaskType {
	case MapTaskType:
		if finish, ok := c.MapFinishMap.Load(args.CurTaskNo); ok {
			if !finish.(Finish).Finish {
				atomic.AddInt32(&c.MapTaskFinishNum, 1)
			}
		}

		log.Printf("get map finish: %v, finishNum: %v", args.CurTaskNo, c.MapTaskFinishNum)
		c.MapFinishMap.Store(args.CurTaskNo, Finish{Finish: true})
		if c.MapTaskFinishNum == c.MapTaskNum {
			log.Printf("======== coordinator turn to Reduce")
			c.State = ReduceTaskType
			var i int32
			for i = 0; i < c.ReduceTaskNum; i++ {
				log.Printf("put reduce task to chan: %v", i)
				c.ReduceTaskChan <- Task{ReduceTaskNo: i}
			}
		}
	case ReduceTaskType:
		if finish, ok := c.ReduceFinishMap.Load(args.CurTaskNo); ok {
			if !finish.(Finish).Finish {
				atomic.AddInt32(&c.ReduceTaskFinishNum, 1)
			}
		}
		log.Printf("get reduce finish: %v, finishNum: %v", args.CurTaskNo, c.ReduceTaskFinishNum)
		c.ReduceFinishMap.Store(args.CurTaskNo, Finish{Finish: true})
		if c.ReduceTaskFinishNum == c.ReduceTaskNum {
			c.State = NoneTaskType
		}
	default:
		log.Printf("***** coordinator get None type")

	}
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

	c.CheckFinish()
	if c.ReduceTaskFinishNum == c.ReduceTaskNum {
		log.Printf("finish check done!!!!!")
		ret = true
	}
	// if c.CheckFinish() {
	// 	ret = true
	// }
	// Your code here.

	return ret
}

func (c *Coordinator) CheckFinish() {
	if c.State == MapTaskType {
		var i int32
		for i = 0; i < c.MapTaskNum; i++ {
			if task, ok := c.MapFinishMap.Load(i); ok {
				nowTime := getTimeStamp()
				if nowTime-task.(Finish).StartTime > 10*1000 && !task.(Finish).Finish {
					log.Printf("<<<<<<<. drop map task: %v", task)
					newTask := Task{
						FileName:  c.files[i],
						MapTaskNo: i,
					}
					log.Printf("<<<<<<<. send new map task: %+v", newTask)
					c.MapTaskChan <- newTask
					c.MapFinishMap.Store(i, Finish{StartTime: getTimeStamp()})
				}
			}
		}
	} else if c.State == ReduceTaskType {
		var i int32
		for i = 0; i < c.ReduceTaskNum; i++ {
			if task, ok := c.ReduceFinishMap.Load(i); ok {
				nowTime := getTimeStamp()
				if nowTime-task.(Finish).StartTime > 10*1000 && !task.(Finish).Finish {
					log.Printf("<<<<<<<. drop reduce task: %+v", task)
					newTask := Task{
						ReduceTaskNo: i,
					}
					log.Printf("<<<<<<<. send new reduce task: %v", newTask)
					c.ReduceTaskChan <- newTask
					c.ReduceFinishMap.Store(i, Finish{StartTime: getTimeStamp()})
				}
			}
		}
	}
}

// func (c *Coordinator) CheckFinish1() bool {
// 	mapFinish, reduceFinish := true, true
// 	if c.State == MapTaskType {
// 		log.Printf("check map task")
// 		c.MapFinishMap.Range(func(k, v interface{}) bool {
// 			if task, ok := v.(Task); ok {
// 				nowtime := getTimeStamp()
// 				mapFinish = false
// 				if nowtime-task.StartTime > 1000*10 && task.Sent {
// 					log.Printf("<<<<<<<. drop map task: %v, k: %v, state: %v", task, k, task.Sent)
// 					// mapFinish = false
// 					// log.Printf("resend map task: %+v", task)
// 					newTask := Task{
// 						FileName:  task.FileName,
// 						MapTaskNo: task.MapTaskNo,
// 						StartTime: getTimeStamp(),
// 						Sent:      true,
// 					}

// 					log.Printf("###########. resend map task: %+v", newTask)
// 					c.MapFinishMap.Store(task.MapTaskNo, Task{
// 						MapTaskNo: task.MapTaskNo,
// 						StartTime: getTimeStamp(),
// 					})
// 					select {
// 					case c.MapTaskChan <- newTask:
// 						c.MapFinishMap.Store(newTask.MapTaskNo, newTask)
// 					case <-time.After(1 * time.Second):
// 						log.Printf("get map chan stuck, taskNo: %v", newTask.MapTaskNo)
// 					}
// 				}
// 			}
// 			return true
// 		})
// 	} else if c.State == ReduceTaskType {
// 		log.Printf("check reduce task")
// 		c.ReduceFinishMap.Range(func(k, v interface{}) bool {
// 			if task, ok := v.(Task); ok {
// 				nowtime := getTimeStamp()
// 				reduceFinish = false
// 				if nowtime-task.StartTime > 1000*10 && task.Sent {
// 					log.Printf("<<<<<<<. drop reduce task: %v, k: %v, state: %v", task, k, task.Sent)
// 					// reduceFinish = false
// 					newTask := Task{
// 						ReduceTaskNo: task.ReduceTaskNo,
// 						StartTime:    getTimeStamp(),
// 						Sent:         true,
// 					}
// 					log.Printf("$$$$$$$$$$$. resend reduce task: %+v", newTask)
// 					c.ReduceFinishMap.Store(task.ReduceTaskNo, Task{
// 						ReduceTaskNo: task.ReduceTaskNo,
// 						StartTime:    getTimeStamp(),
// 					})
// 					select {
// 					case c.ReduceTaskChan <- newTask:
// 						c.ReduceFinishMap.Store(newTask.ReduceTaskNo, newTask)
// 					case <-time.After(1 * time.Second):
// 						log.Printf("get reduce chan stuck, taskNo: %v", newTask.ReduceTaskNo)

// 					}
// 				}
// 			}
// 			return true
// 		})

// 	}
// 	return mapFinish && reduceFinish
// }

func getTimeStamp() int64 {
	return time.Now().UnixMilli()
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.files = files
	c.MapTaskChan = make(chan Task, len(files))
	c.ReduceTaskChan = make(chan Task, nReduce)
	for i := range files {
		mapTask := Task{FileName: files[i], MapTaskNo: int32(i)}
		// c.MapFinishMap.Store(int32(i), Finish{StartTime: getTimeStamp()})
		c.MapTaskChan <- mapTask
	}
	// for i := 0; i < nReduce; i++ {
	// 	reduceTask := Task{ReduceTaskNo: int32(i)}
	// 	c.ReduceFinishMap.Store(int32(i), Finish{StartTime: getTimeStamp()})
	// }
	c.MapTaskNum = int32(len(files))
	c.ReduceTaskNum = int32(nReduce)
	// Your code here.

	c.server()
	return &c
}
