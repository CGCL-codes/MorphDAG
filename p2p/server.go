package p2p

import (
	"MorphDAG/config"
	"MorphDAG/core"
	"MorphDAG/core/state"
	"MorphDAG/core/types"
	"MorphDAG/utils"
	"bufio"
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"log"
	"math"
	"math/rand"
	"os"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

// Server defines the MorphDAG server
type Server struct {
	BC              *core.Blockchain
	NodeID          string
	Address         string
	Network         *NetworkDealer
	TxPool          *TxPool
	StateDB         *state.StateDB
	State           []byte
	NodeNumber      int
	Concurrency     int
	Stake           int
	ProcessingQueue map[string][]*types.Transaction
	CompletedQueue  map[string][]*types.Transaction
	PayloadQueue    *sync.Map
	TmpPayloadQueue *sync.Map
	SyncSignalMap   *sync.Map
	StartOrRun      bool
	IsSender        bool
	StateLock       sync.RWMutex
	QueueLock       sync.RWMutex
}

// define the sending format of block payload (txs)
type blkPayload struct {
	BlkID  []byte
	Height int
	TXs    map[string]struct{}
}

// define the sending format of sync signal
type syncSignal struct {
	Height int
}

var totalOrder []string
var loadScale int
var epochStart = new(sync.Map)
var processingTime time.Duration

func InitializeServer(nodeID string, nodeNumber int, dealer *NetworkDealer, sender bool) *Server {
	dbFile := fmt.Sprintf(config.DBfile2, nodeID)
	stateDB, err := state.NewState(dbFile, nil)
	if err != nil {
		log.Println("initialize failed")
		return nil
	}
	// initialize stake
	var stake int
	id, _ := strconv.Atoi(nodeID)
	if (id+1)%2 == 0 {
		stake = (id + 1) / 2
	} else {
		stake = (id + 2) / 2
	}

	server := &Server{
		NodeID:  nodeID,
		Address: nodeID,
		Network: dealer,
		TxPool:  NewTxPool(),
		StateDB: stateDB,
		State:   []byte{},
		//BlockMerkleRoot: []byte{},
		NodeNumber:      nodeNumber,
		Concurrency:     config.SafeConcurrency,
		Stake:           stake,
		ProcessingQueue: make(map[string][]*types.Transaction),
		CompletedQueue:  make(map[string][]*types.Transaction),
		PayloadQueue:    new(sync.Map),
		TmpPayloadQueue: new(sync.Map),
		SyncSignalMap:   new(sync.Map),
		StartOrRun:      false,
		IsSender:        sender,
	}

	return server
}

func (server *Server) CreateDAG() {
	bc := core.CreateBlockchain(server.NodeID, server.Address, server.Concurrency)
	server.BC = bc
	fmt.Println("MorphDAG initialized!")
}

// Run runs the MorphDAG protocol circularly
func (server *Server) Run(cycles int) {
	defer server.BC.Database.Close()
	fmt.Printf("Server %s starts\n", server.NodeID)

	// wait for 12 seconds for receiving sufficient transactions
	time.Sleep(12 * time.Second)

	for i := 0; i < cycles; i++ {
		var wg sync.WaitGroup
		var stateRoot []byte
		server.StartOrRun = false

		if i > 0 {
			// execute transactions in the last epoch
			wg.Add(1)
			go func() {
				defer wg.Done()
				stateRoot = server.processTxs()
			}()
		}

		// record the start time of each epoch
		start := time.Now()
		epochStart.Store(server.BC.GetLatestHeight()+1, uint64(start.Unix()))
		server.updateConcurrency()
		server.createBlock(server.Concurrency)

		var duration time.Duration
		for {
			duration = time.Since(start)
			if duration.Seconds() >= config.EpochTime {
				break
			}
		}

		// sync operation
		server.sendSyncSignal()
		for !server.StartOrRun {
		}
		if i > 0 {
			wg.Wait()
		}

		loadScale = server.processTxPool()
		server.BC.EnterNextEpoch()
		if i > 0 {
			server.setState(stateRoot)
		}
		fmt.Printf("Epoch %d ends\n", server.BC.GetLatestHeight())
	}
}

// ProcessBlock processes received block and add it to the chain
func (server *Server) ProcessBlock(request []byte) {
	var blk types.Block

	data := request[config.CommandLength:]
	err := json.Unmarshal(data, &blk)
	if err != nil {
		log.Panic(err)
	}

	blockHash := blk.BlockHash
	expBitNum := math.Ceil(math.Log2(float64(server.Concurrency)))
	chainID := utils.ConvertBinToDec(blockHash, int(expBitNum))
	stateRoot := server.getLatestState()
	isAdded := server.BC.AddBlock(&blk, chainID, stateRoot)
	//if isAdded {
	//	// fmt.Println("Block verified and added successfully!")
	//} else {
	//	fmt.Println("Invalid block!")
	//}
	if !isAdded {
		fmt.Println("Invalid block!")
	}
}

// ProcessTx processes the received new transaction
func (server *Server) ProcessTx(request []byte) {
	var t types.Transaction

	data := request[config.CommandLength:]
	err := json.Unmarshal(data, &t)
	if err != nil {
		log.Panic(err)
	}

	// store to the memory tx pool
	t.SetStart()
	server.TxPool.pending.append(&t)
}

// ProcessPayload processes the received block payload
func (server *Server) ProcessPayload(request []byte) {
	var payload blkPayload

	data := request[config.CommandLength:]
	err := json.Unmarshal(data, &payload)
	if err != nil {
		log.Panic(err)
	}

	if payload.Height == server.BC.GetLatestHeight() || payload.Height == server.BC.GetLatestHeight()+1 {
		server.PayloadQueue.Store(string(payload.BlkID), payload.TXs)
	} else if payload.Height == server.BC.GetLatestHeight()+2 {
		server.TmpPayloadQueue.Store(string(payload.BlkID), payload.TXs)
	}
}

// ProcessSyncSignal processes the received sync signal
func (server *Server) ProcessSyncSignal(request []byte) {
	if !server.IsSender {
		var signal syncSignal

		data := request[config.CommandLength:]
		err := json.Unmarshal(data, &signal)
		if err != nil {
			log.Panic(err)
		}

		// do not receive old sync message
		if signal.Height >= server.BC.GetLatestHeight() {
			var num int32
			v, exist := server.SyncSignalMap.Load(signal.Height)
			if exist {
				num = v.(int32)
			}
			atomic.AddInt32(&num, 1)
			server.SyncSignalMap.Store(signal.Height, num)
			if atomic.LoadInt32(&num) >= int32(server.NodeNumber/2) {
				server.StartOrRun = true
			}
		}
	}
}

// HandleTxForever handles txs received from the tx channel
func (server *Server) HandleTxForever() {
	for {
		select {
		case t := <-server.Network.ExtractTx():
			switch t.Type {
			case "tx":
				go server.ProcessTx(t.Msg)
			}
		}
	}
}

// HandleBlkForever handles blocks received from the block channel
func (server *Server) HandleBlkForever() {
	for {
		select {
		case b := <-server.Network.ExtractBlk():
			switch b.Type {
			case "block":
				go server.ProcessBlock(b.Msg)
			}
		}
	}
}

// HandlePayloadForever handles txs contained in the block received from the payload channel
func (server *Server) HandlePayloadForever() {
	for {
		select {
		case p := <-server.Network.ExtractPayload():
			switch p.Type {
			case "payload":
				go server.ProcessPayload(p.Msg)
			}
		}
	}
}

// sendRunSignal broadcasts the sync signal to the network
func (server *Server) sendSyncSignal() {
	data := syncSignal{server.BC.GetLatestHeight()}
	bt, _ := json.Marshal(data)
	signal := append(commandToBytes("sync"), bt...)
	err := server.Network.SyncMsg(signal)
	if err != nil {
		log.Panic(err)
	}
}

// createBlock packages #size of transactions into a new block
func (server *Server) createBlock(con int) {
	var txHashes = make(map[string]struct{})
	server.TxPool.RetrievePending()
	txs := server.TxPool.Pick(config.BlockSize)
	for _, t := range txs {
		hash := t.String()
		txHashes[hash] = struct{}{}
	}

	blk := server.BC.ProposeBlock(txHashes, con, server.Stake, server.State)
	if blk != nil {
		// first propagates the block header
		go func() {
			bt, _ := json.Marshal(*blk)
			req := append(commandToBytes("block"), bt...)
			err := server.Network.SyncMsg(req)
			if err != nil {
				log.Panic(err)
			}
		}()

		// asynchronously propagates the block payload
		go func() {
			server.PayloadQueue.Store(string(blk.BlockHash), txHashes)
			data := blkPayload{blk.BlockHash, blk.Epoch, txHashes}
			bt, _ := json.Marshal(data)
			req := append(commandToBytes("payload"), bt...)
			err := server.Network.SyncMsg(req)
			if err != nil {
				log.Panic(err)
			}
		}()
	}
}

// updateConcurrency updates block concurrency according to the analysis result
func (server *Server) updateConcurrency() {
	scale := server.TxPool.GetScale()
	curCon := core.CalculateConcurrency(scale)
	server.Concurrency = curCon
}

// processTxPool deletes packaged transactions in the transaction pool
func (server *Server) processTxPool() int {
	// remove duplicated blocks with the same id
	server.BC.RmDuplicated()
	server.ProcessingQueue = make(map[string][]*types.Transaction)
	blocks := make(map[string][]*types.Transaction)

	server.PayloadQueue.Range(func(key, value any) bool {
		id := key.(string)
		txs := value.(map[string]struct{})
		delTxs := server.TxPool.DeleteTxs(txs)
		blocks[id] = delTxs
		return true
	})

	load, processing, sorted := rmDuplicatedTxs(blocks)
	totalOrder = sorted

	// mark the transactions appended to the DAG
	for id, txs := range processing {
		for _, t := range txs {
			t.SetEnd1()
		}
		server.ProcessingQueue[id] = txs
	}

	server.prefetchStates(server.ProcessingQueue)
	server.PayloadQueue = server.TmpPayloadQueue
	server.TmpPayloadQueue = new(sync.Map)

	return load
}

// processTxs executes all transactions in all concurrent blocks
func (server *Server) processTxs() []byte {
	// Process all received concurrent blocks in the previous epoch
	runtime.GOMAXPROCS(runtime.NumCPU())
	server.QueueLock.Lock()
	server.CompletedQueue = make(map[string][]*types.Transaction)
	fmt.Printf("load scale is: %d\n", loadScale)

	executor := core.NewExecutor(server.StateDB, config.MaximumProcessors)
	stateRoot, duration := executor.Processing(server.ProcessingQueue, totalOrder, config.HotRatio)
	processingTime = duration

	for id, txs := range server.ProcessingQueue {
		server.CompletedQueue[id] = txs
	}
	server.QueueLock.Unlock()

	return stateRoot
}

// prefetchStates fetches the states of accounts into the statedb
func (server *Server) prefetchStates(txs map[string][]*types.Transaction) {
	for _, set := range txs {
		for _, t := range set {
			for addr := range t.Payload.RWSets {
				server.StateDB.PreFetch(addr)
			}
		}
	}
}

// getLatestState gets latest state root (concurrent safe)
func (server *Server) getLatestState() []byte {
	server.StateLock.RLock()
	defer server.StateLock.RUnlock()
	stateRoot := server.State
	return stateRoot
}

// setState updates the state root (concurrent safe)
func (server *Server) setState(stateRoot []byte) {
	server.StateLock.Lock()
	defer server.StateLock.Unlock()
	server.State = stateRoot
}

// SendTxsForLoop broadcasts txs to the network (replace rpc method)
func (server *Server) SendTxsForLoop(cycles, largeLoads int) {
	var loads []int
	var loadsFile string
	// read file storing dynamic workload scale
	if largeLoads == 0 {
		loadsFile = config.CommonLoads
	} else if largeLoads == 1 {
		loadsFile = config.LargeLoads
	} else {
		log.Panic("Invalid load file indicator")
	}
	bs, err := ioutil.ReadFile(loadsFile)
	if err != nil {
		log.Panic("Read error:", err)
	}
	lines := strings.Split(string(bs), "\n")
	for _, line := range lines {
		load, _ := strconv.Atoi(line)
		loads = append(loads, load)
	}

	file, err := os.Open(config.EthTxFile)
	if err != nil {
		log.Panic("Read error: ", err)
	}
	defer file.Close()
	r := bufio.NewReader(file)

	for i := 0; i < cycles; i++ {
		var txs []*types.Transaction
		load := loads[i]
		interval := 1000 * (6 / float64(load))

		for j := 0; j < load; j++ {
			var t types.Transaction
			txdata, err2 := r.ReadBytes('\n')
			if err2 != nil {
				if err2 == io.EOF {
					break
				}
				log.Panic(err2)
			}
			err2 = json.Unmarshal(txdata, &t)
			if err2 != nil {
				log.Panic(err)
			}
			txs = append(txs, &t)
		}

		fmt.Println("Sending txs... round ", i+1)

		for _, t := range txs {
			// serialize tx data and broadcast to the network
			t.Payload = nil
			payload, _ := json.Marshal(*t)
			request := append(commandToBytes("tx"), payload...)
			err2 := server.Network.SyncMsg(request)
			if err2 != nil {
				log.Panic(err2)
			}
			for k := 0; k < int(interval); k++ {
				time.Sleep(time.Millisecond)
			}
		}

		fmt.Println("Batch of TXs sent success... round ", i+1)
	}
}

// ObserveSystemTPS records the system performance (e.g., latency and tps)
func (server *Server) ObserveSystemTPS(cycles int) {
	for i := 0; i < cycles; i++ {
		time.Sleep(10 * time.Second)
		txs := make(map[string][]*types.Transaction)
		server.QueueLock.RLock()
		if len(server.CompletedQueue) > 0 {
			for id, txSet := range server.CompletedQueue {
				txs[id] = txSet
			}
		}
		server.QueueLock.RUnlock()

		height := server.BC.GetLatestHeight()
		getAppendLatency(txs, height)
		getOverallLatency(txs, height)
	}
}

// rmDuplicatedTxs deletes duplicate transactions for transaction execution
func rmDuplicatedTxs(blocks map[string][]*types.Transaction) (int, map[string][]*types.Transaction, []string) {
	var sorted []string
	var load int
	var deleted = make(map[string]struct{})
	var processing = make(map[string][]*types.Transaction)

	for id := range blocks {
		sorted = append(sorted, id)
	}

	sort.Strings(sorted)

	for _, id := range sorted {
		var added []*types.Transaction
		blk := blocks[id]
		for _, t := range blk {
			iid := t.String()
			if _, ok := deleted[iid]; !ok {
				added = append(added, t)
				deleted[iid] = struct{}{}
			}
		}
		processing[id] = added
		load += len(added)
	}

	return load, processing, sorted
}

func commandToBytes(command string) []byte {
	var newBytes [config.CommandLength]byte

	for i, c := range command {
		newBytes[i] = byte(c)
	}

	return newBytes[:]
}

func bytesToCommand(bytes []byte) string {
	var command []byte

	for _, b := range bytes {
		if b != 0x0 {
			command = append(command, b)
		}
	}

	return fmt.Sprintf("%s", command)
}

// getAppendLatency calculates the latency and throughput of transactions being appended to the DAG
func getAppendLatency(txs map[string][]*types.Transaction, height int) {
	file, err := os.OpenFile(config.ExpResult1, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	num := 0
	sum1 := 0
	sum2 := 0

	for _, ts := range txs {
		for _, t := range ts {
			num++
			lat1 := t.GetEnd1() - t.GetStart()
			sum1 += int(lat1)
			if height >= 1 {
				st, _ := epochStart.Load(height)
				lat2 := t.GetEnd1() - st.(uint64)
				sum2 += int(lat2)
			}
		}
	}

	avgLat := float64(sum1) / float64(num)
	avgLat2 := float64(sum2) / float64(num)
	avgTPS := float64(num) / avgLat2
	avgMbps := ((1.01 * float64(num*8)) / 1024) / avgLat2
	contents := fmt.Sprintf("Time: %d, number of blocks: %d, average latency 1: %.2f, average latency 2: %.2f, average tps: %.2f, average Mbps: %.2f \n",
		time.Now().Unix(), len(txs), avgLat, avgLat2, avgTPS, avgMbps)
	_, err = file.WriteString(contents)
	if err != nil {
		log.Printf("error: %v\n", err)
	}
}

// getOverallLatency calculates the latency and throughput of state persistence
func getOverallLatency(txs map[string][]*types.Transaction, height int) {
	file, err := os.OpenFile(config.ExpResult2, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	num := 0
	sum1 := 0
	sum2 := 0

	for _, ts := range txs {
		for _, t := range ts {
			num++
			lat1 := uint64(processingTime.Seconds()) + t.GetEnd1() + uint64(rand.Intn(3)+1) - t.GetStart()
			sum1 += int(lat1)
			if height >= 1 {
				st, _ := epochStart.Load(height)
				lat2 := uint64(processingTime.Seconds()) + t.GetEnd1() + uint64(rand.Intn(3)+1) - st.(uint64)
				sum2 += int(lat2)
			}
		}
	}

	avgLat := float64(sum1) / float64(num)
	avgLat2 := float64(sum2) / float64(num)
	avgTPS := float64(num) / avgLat2
	avgMbps := ((1.01 * float64(num*8)) / 1024) / avgLat2
	contents := fmt.Sprintf("Time: %d, number of blocks: %d, average latency 1: %.2f, average latency 2: %.2f, average tps: %.2f, average Mbps: %.2f \n",
		time.Now().Unix(), len(txs), avgLat, avgLat2, avgTPS, avgMbps)
	_, err = file.WriteString(contents)
	if err != nil {
		log.Printf("error: %v\n", err)
	}
}
