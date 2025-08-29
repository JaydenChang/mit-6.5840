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

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		log.Printf("prepare to get task")
		GetTask(&args, &reply)
		if reply.TaskType == MapTaskType {
			if reply.Task.FileName == "" {
				continue
			}
			file, err := os.Open(reply.Task.FileName)
			if err != nil {
				log.Printf("<<< open file failed: filename: %v, MapTtaskNo: %v, err %v\n", reply.Task.FileName, reply.Task.MapTaskNo, err)
				continue
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Printf("<<< read file failed: filename: %v, err: %v\n", reply.Task.FileName, err)
				continue
			}
			file.Close()
			kva := mapf(reply.Task.FileName, string(content))
			intermediate := []KeyValue{}
			intermediate = append(intermediate, kva...)
			sort.Sort(ByKey(intermediate))
			bucket := make([][]KeyValue, reply.ReduceTaskNum)

			for i := range intermediate {
				reduceNo := ihash(intermediate[i].Key) % int(reply.ReduceTaskNum)
				bucket[reduceNo] = append(bucket[reduceNo], intermediate[i])
			}
			for i := range bucket {
				tmpFile, _ := os.CreateTemp("./", "mr-tmp")
				enc := json.NewEncoder(tmpFile)
				oname := fmt.Sprintf("mr-%v-%v", reply.Task.MapTaskNo, i)
				for _, kv := range bucket[i] {
					err = enc.Encode(&kv)
					if err != nil {
						log.Println("encode failed: ", err)
						continue
					}
				}
				tmpFile.Close()
				os.Rename(tmpFile.Name(), oname)
			}
			notifyArgs := TaskFinishNotifyArgs{TaskType: MapTaskType, CurTaskNo: reply.Task.MapTaskNo}
			notifyReply := TaskFinishNotifyReply{}
			TaskFinishNotify(&notifyArgs, &notifyReply)
		} else if reply.TaskType == ReduceTaskType {
			intermediate := []KeyValue{}
			for i := 0; i < int(reply.MapTaskNum); i++ {
				mapFilename := fmt.Sprintf("mr-%v-%v", i, reply.Task.ReduceTaskNo)
				mapFile, err := os.Open(mapFilename)
				if err != nil {
					log.Printf("cannot open %v, reduceTaskNo: %v", mapFilename, reply.Task.ReduceTaskNo)
					continue
				}
				dec := json.NewDecoder(mapFile)
				for {
					var kv KeyValue
					if err = dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
			}
			sort.Sort(ByKey(intermediate))

			oname := fmt.Sprintf("mr-out-%v", reply.Task.ReduceTaskNo)
			tmpFile, _ := os.CreateTemp("./", "mr-tmp")
			i := 0
			for i < len(intermediate) {
				j := i + 1
				for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, intermediate[k].Value)
				}
				output := reducef(intermediate[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpFile, "%v %v\n", intermediate[i].Key, output)

				i = j
			}

			tmpFile.Close()
			os.Rename(tmpFile.Name(), oname)

			notifyArgs := TaskFinishNotifyArgs{TaskType: ReduceTaskType, CurTaskNo: reply.Task.ReduceTaskNo}
			notifyReply := TaskFinishNotifyReply{}
			TaskFinishNotify(&notifyArgs, &notifyReply)

		} else if reply.TaskType == NoneTaskType {
			log.Printf("worker get none type")
			break
		} else {
			continue
		}
	}
	// Your worker implementation here.

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()

}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

func GetTask(args *GetTaskArgs, reply *GetTaskReply) {
	if ok := call("Coordinator.GetTask", args, reply); !ok {
		// fmt.Printf("get err\n")
	}
}

func TaskFinishNotify(args *TaskFinishNotifyArgs, reply *TaskFinishNotifyReply) {
	if ok := call("Coordinator.TaskFinishNotify", args, reply); !ok {
		// fmt.Printf("call notify failed: %v\n", args.TaskType)
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
