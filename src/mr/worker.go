package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
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

	// Your worker implementation here.
	for {
		// uncomment to send the Example RPC to the coordinator.
		filenames, worktype, nReduce, workId := CallExample()
		//fmt.Printf("WorkId %v Worktype %v\n", workId, worktype)
		IntermediateFiles := make(map[int]string)
		if worktype == "Done" {

			break
		}
		if worktype == "Wait" {
			time.Sleep(1 * time.Second)
			continue
		}
		if worktype == "Map" {
			intermediate := []KeyValue{}
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				content, err := ioutil.ReadAll(file)
				if err != nil {
					log.Fatalf("cannot read %v", filename)
				}
				file.Close()
				kva := mapf(filename, string(content))
				intermediate = append(intermediate, kva...)
			}
			sort.Sort(ByKey(intermediate))
			for i := 0; i < nReduce; i++ {
				oname := fmt.Sprintf("mr-%v-%v", workId, i)
				_, _ = os.Create(oname)

			}
			for _, kv := range intermediate {
				oname := fmt.Sprintf("mr-%v-%v", workId, ihash(kv.Key)%nReduce)
				ofile, _ := os.OpenFile(oname, os.O_APPEND|os.O_WRONLY|os.O_CREATE, 0644)
				IntermediateFiles[ihash(kv.Key)%nReduce] = oname
				enc := json.NewEncoder(ofile)
				err := enc.Encode(&kv)
				if err != nil {
					log.Fatalf("cannot write to %v", oname)
				}
				ofile.Close()
			}
		}
		if worktype == "Reduce" {
			intermediate := []KeyValue{}
			for _, filename := range filenames {
				file, err := os.Open(filename)
				if err != nil {
					log.Fatalf("cannot open %v", filename)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					intermediate = append(intermediate, kv)
				}
				file.Close()
			}
			sort.Sort(ByKey(intermediate))
			oname := fmt.Sprintf("mr-out-%v", workId)
			ofile, _ := os.Create(oname)
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
				fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)
				i = j
			}
			ofile.Close()
		}
		fmt.Printf("WorkId %v Completed\n", workId)
		CallTaskComplete(&TaskCompleteArgs{
			WorkId:            workId,
			Worktype:          worktype,
			IntermediateFiles: IntermediateFiles,
		})
	}
}

func CallExample() ([]string, string, int, int) {

	// declare an argument structure.
	args := RequestTask{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := AssignTask{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.AssignTask", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v %v\n", reply.WorkId, reply.Worktype)
		return reply.Filename, reply.Worktype, reply.NReduce, reply.WorkId
	} else {
		fmt.Printf("call failed!\n")
		return reply.Filename, reply.Worktype, reply.NReduce, reply.WorkId
	}
}
func CallTaskComplete(args *TaskCompleteArgs) {
	reply := TaskCompleteReply{}
	ok := call("Coordinator.MarkTaskCompleted", args, &reply)
	if ok {
		fmt.Printf("call success!\n")
	} else {
		fmt.Printf("call failed!\n")
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
