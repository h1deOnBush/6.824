package mr

import (
	"fmt"
	"io/ioutil"
	"log"
	"regexp"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"
//


type Master struct {
	// Your definitions here.
	mu sync.RWMutex
	M int
	R int
	idleTasks [2]chan Task
	inProgress [2]map[Task]bool
	completedTasks [2]chan Task
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) GetTask(args ExampleArgs, reply *Task) error {
	if len(m.completedTasks[0]) < m.M {
		select {
		case reply1 := <-m.idleTasks[0]:
			reply.Type = reply1.Type
			reply.Filename = reply1.Filename
			reply.NReduce = reply1.NReduce
			reply.TaskNum = reply1.TaskNum
			reply1.StartTime = time.Now().UnixNano()
			reply.StartTime = reply1.StartTime
			m.mu.Lock()
			m.inProgress[0][reply1] = true
			m.mu.Unlock()
			go m.waitForTask(reply1)
		default:
			reply.EmptyIdle = true
		}
	} else if len(m.completedTasks[1]) < m.R {
		select {
		case reply1 := <-m.idleTasks[1]:
			reply.Type = reply1.Type
			reply.Filename = reply1.Filename
			reply.NReduce = reply1.NReduce
			reply.TaskNum = reply1.TaskNum
			reply1.StartTime = time.Now().UnixNano()
			reply.StartTime = reply1.StartTime
			m.mu.Lock()
			m.inProgress[1][reply1] = true
			m.mu.Unlock()
			go m.waitForTask(reply1)
		default:
			reply.EmptyIdle = true
		}
	} else {
		reply.Type = "done"
	}
	return nil
}

func (m *Master) CompleteTask(args Task, reply *ExampleReply) error {
	m.mu.Lock()
	if args.Type == "map" {
		m.inProgress[0][args] = false
		m.completedTasks[0] <- args
		//fmt.Printf("finish map: %+v\n", args.TaskNum)
		//m.showInProgress(0)
	} else {
		m.inProgress[1][args] = false
		m.completedTasks[1] <- args
		//fmt.Printf("finish reduce:%+v\n", args.TaskNum)
		//m.showInProgress(1)
	}
	m.mu.Unlock()
	return nil
}

func (m* Master) waitForTask(task Task) {
	timer := time.NewTimer(time.Second * 10)
	<-timer.C
	m.mu.Lock()
	if task.Type == "map" {
		if v, ok := m.inProgress[0][task]; ok&&v {
			m.inProgress[0][task] = false
			files, err := ioutil.ReadDir(".")
			if err != nil {
				log.Fatalf("err : %v", err)
			}
			pat := fmt.Sprintf("mr-%d-%%d", task.TaskNum)
			var reduceId int
			for _, file := range files {
				n, err := fmt.Sscanf(file.Name(), pat, &reduceId)
				if n == 1 && err == nil {
					os.Remove(file.Name())
				}
			}
			m.idleTasks[0] <- task
			//fmt.Printf("redo map:%+v\n", task.TaskNum)
			//m.showInProgress(0)
		}
	} else if task.Type == "reduce" {
		if v, ok := m.inProgress[1][task]; ok&&v {
			m.inProgress[1][task] = false
			oname := fmt.Sprintf("mr-out-%d", task.TaskNum)
			os.Remove(oname)
			m.idleTasks[1] <- task
			//fmt.Printf("redo reduce:%+v\n", task.TaskNum)
			//m.showInProgress(1)
		}
	}
	m.mu.Unlock()
}

func delete() {
	files, err := ioutil.ReadDir(".")
	if err != nil {
		log.Fatalf("delete: %v", err)
	}
	for _, file := range files {
		m1, err := regexp.MatchString("mr-\\d-\\d", file.Name())
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		m2, err := regexp.MatchString("mr-out-\\d", file.Name())
		if err != nil {
			log.Fatalf("%v\n", err)
		}
		if m1 || m2 {
			os.Remove(file.Name())
		}
	}
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	//remove file whose name is sockname
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	ret := false

	// Your code here.
	if len(m.completedTasks[0])==m.M && len(m.completedTasks[1])==m.R {
		ret = true
	}
	return ret
}

func (m *Master) showInProgress(i int) {
	fmt.Printf("%d:{", len(m.inProgress[i]))
	for k, v := range m.inProgress[i] {
		fmt.Printf("%v:%v,", k, v)
	}
	fmt.Println("}")
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	delete()
	m := Master {
		mu: sync.RWMutex{},
		M: len(files),
		R: nReduce,
		idleTasks: [2]chan Task{make(chan Task, len(files)), make(chan Task, nReduce)},
		inProgress: [2]map[Task]bool{make(map[Task]bool), make(map[Task]bool)},
		completedTasks: [2]chan Task{make(chan Task, len(files)), make(chan Task, nReduce)},
	}


	// Your code here.
	for i, file := range files {
		task := Task{Type: "map", Filename: file, TaskNum: i, NReduce: nReduce}
		m.idleTasks[0] <- task
	}

	for i:=0; i<nReduce ; i++ {
		m.idleTasks[1] <- Task{
			Type:      "reduce",
			Filename:  "",
			TaskNum:   i,
			NReduce:   nReduce,
		}
	}

	m.server()
	return &m
}

