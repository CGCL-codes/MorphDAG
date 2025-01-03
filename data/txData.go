package data

import (
	"MorphDAG/core/types"
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/DarcyWep/morph-txs"
	"github.com/DarcyWep/pureData"
	"github.com/DarcyWep/pureData/transaction"
	"github.com/syndtr/goleveldb/leveldb"
	"github.com/syndtr/goleveldb/leveldb/opt"
	"io"
	"log"
	"math/big"
	"math/rand"
	"os"
	"strconv"
	"time"
)

type Records struct {
	Rds []Record `json:"RECORDS"`
}

type Record struct {
	Hash        string      `json:"hash"`
	BlockHash   string      `json:"blockHash"`
	BlockNumber string      `json:"blockNumber"`
	Info        Transaction `json:"info"`
}

type Transfer struct {
	From  Balance `json:"from"`
	To    Balance `json:"to"`
	Type  uint8   `json:"type"`
	Nonce uint8   `json:"nonce"`
}

type Balance struct {
	Address  string  `json:"address"`
	Value    float64 `json:"value"`
	BeforeTx float64 `json:"beforeTx"`
	AfterTx  float64 `json:"afterTx"`
}

type Transaction struct {
	Count                uint8      `json:"count"`
	AccessList           string     `json:"accessList"`
	Transfer             []Transfer `json:"balance"`
	BlockHash            string     `json:"blockHash"`
	BlockNumber          string     `json:"blockNumber"`
	ChainId              string     `json:"chainId"`
	From                 string     `json:"from"`
	Gas                  string     `json:"gas"`
	GasPrice             string     `json:"gasPrice"`
	Hash                 string     `json:"hash"`
	Input                string     `json:"input"`
	MaxFeePerGas         string     `json:"maxFeePerGas"`
	MaxPriorityFeePerGas string     `json:"maxPriorityFeePerGas"`
	Nonce                string     `json:"nonce"`
	R                    string     `json:"r"`
	S                    string     `json:"s"`
	To                   string     `json:"to"`
	TransactionIndex     string     `json:"transactionIndex"`
	Type                 string     `json:"type"`
	V                    string     `json:"v"`
	Value                string     `json:"value"`
}

// CreateBatchTxs retrieves tx data by querying database and writes to file
func CreateBatchTxs(filename string, num int) {
	file, err := os.OpenFile(filename, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
		return
	}
	defer file.Close()
	w := bufio.NewWriter(file)

	// open sql server connection
	//morph.SetHost("202.114.7.176")
	//err = morph.OpenSqlServers()
	//if err != nil {
	//	log.Panic(err)
	//}
	//defer morph.CloseSqlServers()

	// open leveldb file
	db, err := openLeveldb(nativeDbPath)
	defer db.Close()
	if err != nil {
		fmt.Println("open leveldb error,", err)
		return
	}

	// starting block number
	blockNumber := 14000000

	for i := blockNumber; i < (blockNumber + num); i++ {
		number := new(big.Int).SetInt64(int64(i))
		mtxs, _ := pureData.GetTransactionsByNumber(db, number)
		for index, tx := range mtxs {
			if index < len(mtxs)-1 {
				trans := tx.Transfers
				payload := createNewPayloads(trans)
				if len(payload.RWSets) == 0 {
					continue
				}
				t := types.NewTransaction(0, []byte("FromAddress"), []byte("ToAddress"), payload)
				txdata, _ := json.Marshal(t)
				_, err = w.WriteString(fmt.Sprintf("%s\n", string(txdata)))
				if err != nil {
					log.Printf("error: %v\n", err)
				}
			}
		}
		w.Flush()
	}
}

// ReadEthTxsFile reads tx data from file and deserializes them (for test)
func ReadEthTxsFile(readName string) []*types.Transaction {
	var txs []*types.Transaction

	file, err := os.Open(readName)
	if err != nil {
		log.Panic("Read error: ", err)
	}
	defer file.Close()

	r := bufio.NewReader(file)

	for {
		txdata, err := r.ReadBytes('\n')
		if err != nil {
			if err == io.EOF {
				break
			} else {
				log.Panic(err)
			}
		}

		var tx types.Transaction
		err = json.Unmarshal(txdata, &tx)
		if err != nil {
			log.Panic(err)
		}

		txs = append(txs, &tx)
	}

	return txs
}

func createPayloads(trans []*morph.MorphTransfer) *types.Payload {
	var payload = make(map[string][]*types.RWSet)

	for _, tran := range trans {
		switch tran.Type {
		case 1:
			rand.Seed(time.Now().UnixNano())
			random := rand.Intn(3)
			if random == 0 {
				// read-write (withdraw)
				r := rand.Intn(20)
				rw1 := &types.RWSet{Label: "r", Addr: []byte("1")}
				rw2 := &types.RWSet{Label: "w", Addr: []byte("1"), Value: int64(-r)}
				addr1 := tran.To
				payload[addr1] = append(payload[addr1], rw1, rw2)
				rw3 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
				addr2 := tran.From
				payload[addr2] = append(payload[addr2], rw3)
			} else if random == 1 {
				// only read (query)
				rw := &types.RWSet{Label: "r", Addr: []byte("0")}
				addr := tran.To
				payload[addr] = append(payload[addr], rw)
			} else {
				// incremental write
				r := rand.Intn(20)
				rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
				addr := tran.To
				payload[addr] = append(payload[addr], rw)
			}
		case 2:
			// create contract
			rw := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(200000)}
			addr := tran.To
			payload[addr] = append(payload[addr], rw)
		case 3:
			// transfer transaction fee to the miner
			r := rand.Intn(10)
			rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
			addr := tran.To
			payload[addr] = append(payload[addr], rw)
		case 4:
			// return gas fee to the caller
			r := rand.Intn(5)
			rw := &types.RWSet{Label: "iw", Addr: []byte("1"), Value: int64(r)}
			addr := tran.To
			payload[addr] = append(payload[addr], rw)
		case 5:
			// withhold transaction fee from the caller
			r := rand.Intn(10)
			rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
			rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
			addr := tran.From
			payload[addr] = append(payload[addr], rw1, rw2)
		case 6:
			// destroy the contract
			rw := &types.RWSet{Label: "w", Addr: []byte("2"), Value: int64(0)}
			addr := tran.To
			payload[addr] = append(payload[addr], rw)
		case 7, 8:
			// mining reward
			r := rand.Intn(100)
			rw := &types.RWSet{Label: "iw", Addr: []byte("1"), Value: int64(r)}
			addr := tran.To
			payload[addr] = append(payload[addr], rw)
		default:
			continue
		}
	}

	return &types.Payload{RWSets: payload}
}

// createNewPayloads creates tx payloads (after block height 12,000,000)
func createNewPayloads(trans []transaction.Transfer) *types.Payload {
	var payload = make(map[string][]*types.RWSet)

	for _, tran := range trans {
		if tran.GetLabel() == 0 {
			state, _ := tran.(*transaction.StateTransition)
			switch state.Type {
			case 1:
				// read-write (common transfer)
				r := rand.Intn(40)
				rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
				rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
				addr1 := state.From.Address.String()
				payload[addr1] = append(payload[addr1], rw1, rw2)
				rw3 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
				addr2 := state.To.Address.String()
				payload[addr2] = append(payload[addr2], rw3)
			case 2:
				// withhold transaction fee from the caller
				r := rand.Intn(10)
				rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
				rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: int64(-r)}
				addr := state.From.Address.String()
				payload[addr] = append(payload[addr], rw1, rw2)
			//case 3:
			//	// transfer transaction fee to the miner
			//	r := rand.Intn(10)
			//	rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
			//	addr := state.To.Address.String()
			//	payload[addr] = append(payload[addr], rw)
			case 4:
				// destroy contract
				rw := &types.RWSet{Label: "w", Addr: []byte("1"), Value: int64(0)}
				addr := state.To.Address.String()
				payload[addr] = append(payload[addr], rw)
			//case 5:
			//	// mining reward
			//	r := rand.Intn(20)
			//	rw := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: int64(r)}
			//	addr := state.To.Address.String()
			//	payload[addr] = append(payload[addr], rw)
			default:
				continue
			}
		} else {
			storage, _ := tran.(*transaction.StorageTransition)
			if storage.NewValue == nil {
				// sload (read operation)
				a := strconv.Itoa(rand.Intn(3) + 2)
				rw := &types.RWSet{Label: "r", Addr: []byte(a)}
				addr := storage.Contract.String()
				payload[addr] = append(payload[addr], rw)
			} else {
				// sstore (write operation)
				if storage.PreValue.Big().Cmp(storage.NewValue.Big()) == 1 {
					// deduce operation
					r := rand.Intn(20)
					a := strconv.Itoa(rand.Intn(3) + 2)
					rw1 := &types.RWSet{Label: "r", Addr: []byte(a)}
					rw2 := &types.RWSet{Label: "w", Addr: []byte(a), Value: int64(-r)}
					addr := storage.Contract.String()
					payload[addr] = append(payload[addr], rw1, rw2)
				} else {
					// incremental operation
					r := rand.Intn(30)
					a := strconv.Itoa(rand.Intn(3) + 2)
					rw := &types.RWSet{Label: "iw", Addr: []byte(a), Value: int64(r)}
					addr := storage.Contract.String()
					payload[addr] = append(payload[addr], rw)
				}
			}
		}
	}
	return &types.Payload{RWSets: payload}
}

const (
	minCache     = 2048
	minHandles   = 2048
	nativeDbPath = "/Volumes/ETH_DATA/newdata/nativedb"
)

func openLeveldb(path string) (*leveldb.DB, error) {
	return leveldb.OpenFile(path, &opt.Options{
		OpenFilesCacheCapacity: minHandles,
		BlockCacheCapacity:     minCache / 2 * opt.MiB,
		WriteBuffer:            minCache / 4 * opt.MiB, // Two of these are used internally
		ReadOnly:               true,
	})
}
