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
	"sync/atomic"
	"time"
)

type Coordinator struct {
	nReduce int

	newTaskIdsChan chan int
	mapInput       map[int]string   // map an original id of a map task to its input file
	reduceInputs   map[int][]string // map an original id of a reduce task to its input files
	taskInfos      map[int]struct {
		status     int
		updatedAt  time.Time
		originalId int
	} // map an ephemeral id of a task to its status
	mapsMutex    *sync.Mutex
	currentMaxId *atomic.Int32

	outputs []string
	done    chan struct{}
}

const (
	NotStarted = iota
	InProgress
	Completed
	Expired
)

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mapsMutex.Lock()

	if info, ok := c.taskInfos[args.TaskId]; ok && info.status == InProgress {
		if isMapTask(args.TaskId) {
			// Is a map task
			for _, output := range args.Outputs {
				// output filenames have the format "mr-X-Y" where Y is the reduce task id
				reduceId, err := strconv.Atoi(output[strings.LastIndex(output, "-")+1:])
				if err != nil {
					return err
				}
				// append the output file to the reduce task's input file list
				c.reduceInputs[reduceId] = append(c.reduceInputs[reduceId], output)
			}
			// this map task is done, remove it from the mapInput list
			delete(c.mapInput, info.originalId)
		} else {
			// Is a reduce task
			c.outputs = append(c.outputs, args.Outputs...)
			// this reduce task is done, remove it from the reduceInputs list
			delete(c.reduceInputs, info.originalId)
		}

		info.status = Completed
		c.taskInfos[args.TaskId] = info

		log.Printf("[Coordinator] Task %d completed\n", args.TaskId)

		if len(c.mapInput) == 0 {
			if len(c.reduceInputs) == 0 {
				// all tasks are done
				log.Printf("[Coordinator] All tasks are done\n")
				close(c.newTaskIdsChan)
				close(c.done)
			} else if len(c.reduceInputs) == c.nReduce {
				// all map tasks are done, we enter reduce tasks phase
				for id := 1; id <= c.nReduce; id++ {
					c.newTaskIdsChan <- id
				}
			}
		}
	}
	c.mapsMutex.Unlock()

	taskId, ok := <-c.newTaskIdsChan
	if !ok {
		reply.ExitSignal = true
		log.Printf("[Coordinator] No more tasks, signaling worker to exit\n")
		return nil
	}

	c.mapsMutex.Lock()
	defer c.mapsMutex.Unlock()

	originalId := c.taskInfos[taskId].originalId

	if isMapTask(taskId) {
		input := c.mapInput[originalId]
		reply.Map = &MapTask{
			Id:                taskId,
			InputFile:         input,
			NReduce:           c.nReduce,
			OutputFilenameFmt: fmt.Sprintf("mr-%d-%%d", originalId),
		}
		c.taskInfos[taskId] = struct {
			status     int
			updatedAt  time.Time
			originalId int
		}{
			status:     InProgress,
			updatedAt:  time.Now(),
			originalId: originalId,
		}
		log.Printf("[Coordinator] Assigning map task %d with input file %s\n", taskId, input)
	} else {
		inputs := c.reduceInputs[originalId]
		reply.Reduce = &ReduceTask{
			Id:             taskId,
			InputFiles:     inputs,
			OutputFilename: fmt.Sprintf("mr-out-%d", originalId),
		}
		c.taskInfos[taskId] = struct {
			status     int
			updatedAt  time.Time
			originalId int
		}{
			status:     InProgress,
			updatedAt:  time.Now(),
			originalId: originalId,
		}
		log.Printf("[Coordinator] Assigning reduce task %d with input files %v\n", taskId, inputs)
	}

	return nil
}

// must run in goroutine
func (c *Coordinator) resetSlowTask() {
	for {
		select {
		case <-c.Wait():
			return
		case <-time.After(5 * time.Second):
			c.mapsMutex.Lock()

			var resetTaskIds []int
			for id, info := range c.taskInfos {
				if info.status == InProgress && time.Since(info.updatedAt) > 10*time.Second {
					resetTaskIds = append(resetTaskIds, id)

					info.status = Expired
					info.updatedAt = time.Now()
					c.taskInfos[id] = info
				}
			}

			if len(resetTaskIds) > 0 {
				log.Printf("[Coordinator] Resetting slow tasks: %v\n", resetTaskIds)
			}

			var newTaskIds []int
			for _, id := range resetTaskIds {
				newId := c.currentMaxId.Add(1)
				if id < 0 {
					newId = -newId
				}

				c.taskInfos[int(newId)] = struct {
					status     int
					updatedAt  time.Time
					originalId int
				}{
					status:     NotStarted,
					updatedAt:  time.Now(),
					originalId: c.taskInfos[id].originalId,
				}
				newTaskIds = append(newTaskIds, int(newId))
			}

			c.mapsMutex.Unlock()

			// push to channel outside the lock to avoid deadlock
			for _, id := range newTaskIds {
				c.newTaskIdsChan <- id
			}
		}
	}
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

func (c *Coordinator) Wait() <-chan struct{} {
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{
		nReduce: nReduce,

		mapInput:     make(map[int]string),
		reduceInputs: make(map[int][]string),
		taskInfos: make(map[int]struct {
			status     int
			updatedAt  time.Time
			originalId int
		}),
		outputs:        []string{},
		mapsMutex:      &sync.Mutex{},
		currentMaxId:   &atomic.Int32{},
		done:           make(chan struct{}),
		newTaskIdsChan: make(chan int, len(files)+nReduce),
	}

	// NOTE: negative id is for map tasks, positive id is for reduce tasks

	for i, file := range files {
		c.mapInput[i] = file
		c.taskInfos[-(i + 1)] = struct {
			status     int
			updatedAt  time.Time
			originalId int
		}{status: NotStarted, updatedAt: time.Now(), originalId: i}

		c.newTaskIdsChan <- -(i + 1)
	}

	for i := 0; i < nReduce; i++ {
		c.reduceInputs[i] = []string{}
		c.taskInfos[i+1] = struct {
			status     int
			updatedAt  time.Time
			originalId int
		}{status: NotStarted, updatedAt: time.Now(), originalId: i}
	}

	if len(files) > nReduce {
		c.currentMaxId.Store(int32(len(files)))
	} else {
		c.currentMaxId.Store(int32(nReduce))
	}

	log.Printf("[Coordinator] Created coordinator with %d map tasks and %d reduce tasks\n", len(files), nReduce)

	c.server()
	go c.resetSlowTask()

	return &c
}

func isMapTask(taskId int) bool {
	return taskId < 0
}
