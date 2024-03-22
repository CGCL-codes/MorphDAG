# MorphDAG-Lite
This repository contains the source code and the experiment scripts of MorphDAG-Lite, a lite version of an elastic DAG-based Blockchain. It provides an elastic DAG storage model and a dual-mode transaction processing mechanism to achieve elastic storage and transaction processing.
For more technical details, please refer to the paper [IEEE TKDE'24] MorphDAG: A Workload-Aware Elastic DAG-based Blockchain

## Build & Usage

### 1. Machine types

Machines are divided into two types:

- Severs: run daemons of MorphDAG (`launch/start_server.go`), communicate with each other via P2P model
- Clients: run client programs of MorphDAG (`launch/start_client.go`), communicate with `server` via RPC model

### 2. How to build

In the sub-directory `launch`, compile the node network configuration file `node_config.go` and the server and client programs `start_server.go` and `start_client.go`. 

On Mac OS:

```
cd launch
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build node_config.go
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build start_server.go
CGO_ENABLED=0 GOOS=darwin GOARCH=amd64 go build start_client.go
```

For Apple M1 or M2 chip, you should use `GOARCH=arm64`.

On Windows:

```
cd launch
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build node_config.go
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build start_server.go
CGO_ENABLED=0 GOOS=windows GOARCH=amd64 go build start_client.go
```

On Linux:

```
cd launch
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build node_config.go
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build start_server.go
CGO_ENABLED=0 GOOS=linux GOARCH=amd64 go build start_client.go
```

## Deploy & Run

All the experimental scripts are located in the sub-directory `./experiments`.

- #### Run in a local node environment

Step 1: configure the node connection file

```
./nodeconfig_local.sh $num
```

`$num` denotes the number of MorphDAG nodes run locally. Generated node address files are stored in `../Prototype/nodefile`.

Step 2: run the daemon (server program) of MorphDAG

```
./server_local.sh $num $cycles
```

`$cycles` denotes the number of cycles MorphDAG servers run. It will take 2~3 minutes for node discovery and connections. After all server processes print `Server $id starts` on the terminal , please run the client program immediately.

Step 3: run the client program of MorphDAG

```
./client_local.sh $send $large $observe
```

`$send` denotes the number of transaction sending cycles. `$large` denotes whether to use the large workload size (0 is the Ethereum workload size and 1 is the large workload size). `loads.txt` depicts the Ethereum workload size, and `large_loads.txt` depicts the large workload size. You can modify `large_loads.txt` to change the transaction sending rate (each row represents the number of transactions sent per second). $observe denotes the number of cycles run by anther client process to observe system tps and latency.

Step 4: kill the server and client programs

```
./kill_local.sh
```

After running the speicific number of cycles, the server program can automatically stop, or you can dircetly kill the server and client programs. You can find two experimental results in the current directory, where `tps_appending_result.txt` presents the appending tps and latency of MorphDAG, and `tps_overall_result.txt`  presents the system overall tps and latency.

- #### Run in a distributed node environment

Step 1: modify `hosts.txt` and deploy compiled codes to each remote node

```
./deploy.sh
```

Please enter the ip address of each remote node in `hosts.txt`. Notice that you should enable your work computer to login in these remote nodes without passwords in advance.

Step 2: configure the node connection file

```
./nodeconfig.sh $num
```

`$num` denotes the number of MorphDAG nodes run in a distributed environment. Generated node address files are stored in `../nodefile/ecs/` in the local, as well as in `~/MorphDAG/nodefile` in each remote node.

Step 3: run the daemon (server program) of MorphDAG

```
./server.sh $num $cycles $large
```

In a distributed environment, we directly use the daemon program for tx sending and performance observation (pick two different servers for performing those). `$num` denotes the number of running MorphDAG servers. `$cycles` denotes the number of cycles MorphDAG servers run and the number of cycles of tx sending. `$large` denotes whether to use the large workload size (0 is the Ethereum workload size and 1 is the large workload size).

Step 4: kill the server programs

```
./kill.sh
```

Step 5: download the experimental results

```
./download.sh
```

Since the observation results are stored on the remote machine who runs the server program, please download the relevant files `tps_appending_result.txt` and `tps_overall_result.txt` from the server side.
