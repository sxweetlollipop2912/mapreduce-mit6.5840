# mapreduce-mit6.5840

A simple Go implementation of a distributed MapReduce framework, based on the [Google MapReduce paper](http://research.google.com/archive/mapreduce-osdi04.pdf).

For context, the MapReduce design simplifies large-scale data processing. The user will 
provide 2 functions: `map` and `reduce`.
- The `map` function reads data and emits a set of intermediate key-value 
pairs.
- The `reduce` function collects all intermediate values for a given key 
and "reduces" them to a final output.

While this project is only a lab submission for MIT Distributed Systems 
course, its ambitious goal is to provide the distributed system that 
handles (1) task scheduling, (2) data partitioning, and (3) fault 
tolerance, allowing the user to focus on the logic of the `map` and 
`reduce` functions.

## Architecture & Features

The system uses a coordinator/worker architecture communicating via RPC.

*   **Coordinator**: A single master process responsible for:
    *   **Task Scheduling**: Assigns map and reduce tasks to workers. Map tasks are created for each input file and run to completion before reduce tasks are scheduled.
    *   **Fault Tolerance**: If a worker doesn't complete a task within a timeout (e.g., 10 seconds), the coordinator reassigns the task to another worker.

*   **Worker**: Executes `map` and `reduce` tasks assigned by the coordinator.

*   **Data Flow**:
    1.  `map` tasks read input files and partition their output into intermediate files (`mr-X-Y`) using a hash function on the keys. `X` is the map task ID, and `Y` is the reduce task ID.
    2.  `reduce` tasks read their corresponding intermediate files, process the data, and write final output to `mr-out-Y` files.

Communication is handled via RPC over Unix domain sockets this simple prototype to run on 1 machine.

To run coordinator and workers on separate
machines, as they would in practice, we need to (1) set up
RPCs to communicate over TCP/IP instead of Unix sockets, and (2) read/write files using a shared file system.

## Testing

To verify the implementation, including parallelism and fault tolerance, run the test script:

```bash
# Navigate to the main directory
cd main

# Run the test script on all test cases
sh ./test-mr.sh
```

A successful run will end with the output `*** PASSED ALL TESTS`.

## RPC Communication

Workers and the coordinator communicate via a single RPC 
method, `Coordinator.RequestTask` (see `mr/rpc.go`), which allows a worker to report its previous task's completion and request a new one in a single call.

*   **Worker -> Coordinator (`RequestTaskArgs`)**: A worker sends its completed task ID and a list of output files. A task ID of 0 indicates a new worker.
*   **Coordinator -> Worker (`RequestTaskReply`)**: The coordinator responds with a new map/reduce task or a signal to exit if the job is complete.
