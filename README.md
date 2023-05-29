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

### 3. Fetch Ethereum dataset

We capture Ethereum transaction data by running an Ethereum full node, and convert the transaction data into the transaction format of MorphDAG through converters. In the sub-directory `data`, compile the transaction fetcher file `txData.go` and run:

```
cd data
go build txData.go
./txData -b=$blockNum
```

`$blockNum` denotes the number of blocks you want to fetch. Lastly, the Ethereum transaction dataset is stored in `EthTxs.txt`.
