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
	"strconv"
	"strings"
	"time"
)


//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }
//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.
	for {
		var reply Task
		call("Master.GetTask", ExampleArgs{}, &reply)
		if !reply.EmptyIdle {
			// TODO FAULT TOLERANCE
			//reply.StartTime = time.Now()
			if reply.Type == "map" {
				doMap(reply, mapf)
				call("Master.CompleteTask", reply, &ExampleReply{})

			} else if reply.Type == "reduce" {
				doReduce(reply, reducef)
				call("Master.CompleteTask", reply, &ExampleReply{})

			} else {
				return
			}
		} else {
			time.Sleep(time.Second)
		}
	}
}


//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	//同步阻塞调用
	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Printf("err:%v\n", err)
	return false
}

func doMap(task Task, mapf func(string, string) []KeyValue) {
	reduces := make([][]KeyValue, task.NReduce)
	//dir, _ := os.Getwd()
	//fmt.Println(dir)
	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v", task.Filename)
	}
	file.Close()
	kva := mapf(task.Filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		reduces[idx] = append(reduces[idx], kv)
	}
	for i, reduce := range reduces {
		oname := fmt.Sprintf("mr-%d-%d", task.TaskNum, i)
		file, err := os.OpenFile(oname, os.O_CREATE | os.O_RDWR | os.O_APPEND, 0777)
		if err != nil {
			log.Fatalf("cannot open %v, err: %v\n", oname, err)
		}
		defer file.Close()
		enc := json.NewEncoder(file)
		for _, kv := range reduce {
			enc.Encode(&kv)
		}
	}
}

func doReduce(task Task, reducef func(string, []string) string) {
	kva := []KeyValue{}
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("err : %v", err)
	}
	for _, file := range files {
		if strings.Compare(string(file.Name()[len(file.Name())-1]), strconv.Itoa(task.TaskNum)) ==0 {

			f, err := os.Open(file.Name())
			if err != nil {
				log.Fatalf("cannot open file: %v", f.Name())
			}
			defer f.Close()
			dec := json.NewDecoder(f)
			for {
				var kv KeyValue
				if err := dec.Decode(&kv); err != nil {
					break
				}
				kva = append(kva, kv)
			}
		}
	}
	sort.Sort(ByKey(kva))
	oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
	ofile, _ := os.Create(oname)


	//
	// call Reduce on each distinct key in kva[],
	// and print the result to mr-out-0.
	//
	i := 0
	for i < len(kva) {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

		i = j
	}

	ofile.Close()
}