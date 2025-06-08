package mr

import (
	"fmt"
	"hash/fnv"
	"io"
	"log"
	"net/rpc"
	"os"
	"slices"
	"strings"
	"time"
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(
	mapf func(string, string) []KeyValue,
	reducef func(string, []string) string,
) {
	requestTaskArgs := RequestTaskArgs{}
	for {
		log.Printf("[Worker] Requesting task\n")

		requestTaskReply := RequestTaskReply{}
		if !rpcWithRetry("Coordinator.RequestTask", &requestTaskArgs, &requestTaskReply) {
			log.Fatal("RequestTask failed")
		}
		if requestTaskReply.ExitSignal {
			break
		}

		log.Printf("[Worker] Received task: %+v\n", requestTaskReply)
		requestTaskArgs = RequestTaskArgs{}

		if requestTaskReply.Map != nil {
			outputs, err := mapJob(mapf, requestTaskReply.Map.InputFile, requestTaskReply.Map.OutputFilenameFmt, requestTaskReply.Map.NReduce)
			if err != nil {
				log.Printf("[Worker] Error running map task: input: %v, error %v\n", requestTaskReply.Map.InputFile, err)
				// abandon current task, ask for a new one
				continue
			}
			requestTaskArgs = RequestTaskArgs{TaskId: requestTaskReply.Map.Id, Outputs: outputs}
		} else if requestTaskReply.Reduce != nil {
			output, err := reduceJob(reducef, requestTaskReply.Reduce.InputFiles, requestTaskReply.Reduce.OutputFilename, requestTaskReply.Reduce.Id)
			if err != nil {
				log.Printf("[Worker] Error running reduce task: input: %v, error %v\n", requestTaskReply.Reduce.InputFiles, err)
				// abandon current task, ask for a new one
				continue
			}
			requestTaskArgs = RequestTaskArgs{TaskId: requestTaskReply.Reduce.Id, Outputs: []string{output}}
		}
	}
}

func mapJob(mapf func(string, string) []KeyValue, iPath string, oFilenameFmt string, nReduce int) ([]string, error) {
	iFile, err := os.Open(iPath)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot open %v", err, iPath)
	}
	defer iFile.Close()

	content, err := io.ReadAll(iFile)
	if err != nil {
		return nil, fmt.Errorf("%w: cannot read %v", err, iPath)
	}

	kva := mapf(iPath, string(content))

	// Partition key-value pairs into nReduce intermediate pairs
	intermediateKV := make([][]KeyValue, nReduce)
	for _, kv := range kva {
		reduceId := ihash(kv.Key) % nReduce
		intermediateKV[reduceId] = append(intermediateKV[reduceId], kv)
	}

	// Write intermediate files
	var oFilenames []string
	for i, kv := range intermediateKV {
		oFile, err := os.CreateTemp(".", fmt.Sprintf(oFilenameFmt+"-*", i))
		if err != nil {
			return nil, fmt.Errorf("%w: cannot create temp file for intermediate kv of %d bucket", err, i)
		}
		defer oFile.Close()

		// sort intermediate key values
		slices.SortFunc(kv, func(a, b KeyValue) int {
			return strings.Compare(a.Key, b.Key)
		})
		for _, kv := range kv {
			if _, err := fmt.Fprintf(oFile, "%v %v\n", kv.Key, kv.Value); err != nil {
				return nil, fmt.Errorf("%w: cannot write to %v", err, oFile.Name())
			}
		}

		oname := fmt.Sprintf(oFilenameFmt, i)
		if err = os.Rename(oFile.Name(), oname); err != nil {
			return nil, fmt.Errorf("%w: cannot rename %v", err, oname)
		}
		oFilenames = append(oFilenames, oname)
	}

	return oFilenames, nil
}

func reduceJob(reducef func(string, []string) string, iPaths []string, oFilename string, reduceId int) (string, error) {
	// Read & sort intermediate key values
	kv := make(map[string][]string)
	for _, iPath := range iPaths {
		iFile, err := os.Open(iPath)
		if err != nil {
			return "", fmt.Errorf("%w: cannot open %v", err, iPath)
		}
		defer iFile.Close()

		content, err := io.ReadAll(iFile)
		if err != nil {
			return "", fmt.Errorf("%w: cannot read %v", err, iPath)
		}

		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			parts := strings.Split(line, " ")
			if len(parts) == 2 {
				if _, ok := kv[parts[0]]; !ok {
					kv[parts[0]] = []string{}
				}
				kv[parts[0]] = append(kv[parts[0]], parts[1])
			}
		}
	}

	// Run reducef and write to output file
	oFile, err := os.CreateTemp(".", oFilename+"-*")
	if err != nil {
		return "", fmt.Errorf("%w: cannot create %v", err, oFilename)
	}
	defer oFile.Close()

	for key, values := range kv {
		output := reducef(key, values)
		if _, err := fmt.Fprintf(oFile, "%v %v\n", key, output); err != nil {
			return "", fmt.Errorf("%w: cannot write to %v", err, oFilename)
		}
	}

	if err = os.Rename(oFile.Name(), oFilename); err != nil {
		return "", fmt.Errorf("%w: cannot rename %v", err, oFilename)
	}

	return oFilename, nil
}

func rpcWithRetry(rpcname string, args any, reply any) bool {
	for i := 1; i <= 10; i++ {
		if call(rpcname, args, reply) {
			return true
		}
		if i < 10 {
			time.Sleep(time.Second)
		}
	}
	return false
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
