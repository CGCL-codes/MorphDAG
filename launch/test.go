package main

import (
	"MorphDAG/core"
	"MorphDAG/core/state"
	"MorphDAG/core/tp"
	"MorphDAG/core/types"
	"MorphDAG/nezha"
	"flag"
	"fmt"
	"github.com/chinuy/zipf"
	"math/rand"
	"runtime"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"
)

const dbFile1 = "../data/Morph_Test"
const dbFile2 = "../data/Morph_Test2"
const dbFile3 = "../data/Morph_Test3"

func main() {
	var addrNum uint64
	var blkNum int
	var skew float64
	var topk float64
	var ratio int

	flag.Uint64Var(&addrNum, "a", 100000, "specify address number to use. defaults to 10000.")
	flag.IntVar(&blkNum, "b", 16, "specify transaction number to use. defaults to 5.")
	flag.Float64Var(&skew, "s", 1.0, "specify skew to use. defaults to 0.2.")
	flag.Float64Var(&topk, "k", 0.01, "specify the threshold to identify hotspot accounts. defaults to 0.01.")
	flag.IntVar(&ratio, "r", 6, "specify the read-write ratio. defaults to 3.")
	flag.Parse()

	//data.CreateBatchTxs("./data/newEthTxs2.txt", 10000)
	//txs := data.ReadEthTxsFile("./data/newEthTxs1.txt")
	//fmt.Println(len(txs))

	//file, err := os.Open("./data/newEthTxs2.txt")
	//if err != nil {
	//	log.Panic("Read error: ", err)
	//}
	//defer file.Close()
	//r := bufio.NewReader(file)
	//
	//var ttxs = make(map[string][]*types.Transaction)
	//var writesum int
	//var iwsum int
	//var readsum int
	//
	//for i := 0; i < 40; i++ {
	//	var txs []*types.Transaction
	//	for j := 0; j < 1000; j++ {
	//		var tx types.Transaction
	//		txdata, err2 := r.ReadBytes('\n')
	//		if err2 != nil {
	//			if err2 == io.EOF {
	//				break
	//			}
	//			log.Panic(err2)
	//		}
	//		err2 = json.Unmarshal(txdata, &tx)
	//		if err2 != nil {
	//			log.Panic(err)
	//		}
	//		txs = append(txs, &tx)
	//
	//		for _, sets := range tx.Data().RWSets {
	//			for _, rw := range sets {
	//				if rw.Label == "w" || rw.Label == "iw" {
	//					writesum++
	//					if rw.Label == "iw" {
	//						iwsum++
	//						readsum++
	//					}
	//				} else if rw.Label == "r" {
	//					readsum++
	//				}
	//			}
	//		}
	//	}
	//	ttxs[strconv.Itoa(i)] = txs
	//}

	//fmt.Println(readsum)
	//fmt.Println(writesum)
	//fmt.Println(iwsum)
	//fmt.Println(float64(writesum) / float64(writesum+readsum))
	//fmt.Println(float64(readsum) / float64(writesum+readsum))
	//fmt.Println(float64(iwsum) / float64(writesum))
	//
	//hotAccounts := core.AnalyzeHotAccounts(ttxs, 0.01, false, false)
	//
	//var accesses = make(map[string]int)
	//
	//for _, txs := range ttxs {
	//	for _, tx := range txs {
	//		load := tx.Data().RWSets
	//		for addr := range load {
	//			accesses[addr]++
	//		}
	//	}
	//}
	//
	//var haccess int
	//var caccess int
	//var hotnum int
	//var coldnum int
	//
	//for addr, freq := range accesses {
	//	if _, ok := hotAccounts[addr]; ok {
	//		haccess += freq
	//		hotnum++
	//	} else {
	//		caccess += freq
	//		coldnum++
	//	}
	//}
	//
	//fmt.Println(hotnum)
	//fmt.Println(coldnum)
	//fmt.Println(haccess)
	//fmt.Println(caccess)
	//fmt.Println(float64(caccess) / float64(coldnum))
	//fmt.Println(float64(haccess) / float64(hotnum))

	//var result []int
	//var res int
	//for i := 0; i < 100; i++ {
	//	if i > 0 {
	//		res = result[i-1]
	//	}
	//	if i < 99 {
	//		for j := i * 631; j < (i+1)*631; j++ {
	//			res += accessList[j]
	//		}
	//	} else {
	//		for j := 99 * 631; j < 63122; j++ {
	//			res += accessList[j]
	//		}
	//	}
	//	result = append(result, res)
	//}

	//file2, err3 := os.OpenFile("fraction.txt", os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	//if err3 != nil {
	//	fmt.Printf("error: %v\n", err3)
	//}
	//defer file2.Close()
	//w := bufio.NewWriter(file2)
	//
	//for _, v := range result {
	//	frac := 100 * (float64(v) / float64(sum))
	//	_, err4 := w.WriteString(fmt.Sprintf("%.2f\n", frac))
	//	if err4 != nil {
	//		log.Printf("error: %v\n", err4)
	//	}
	//}
	//w.Flush()

	txs := CreateBatchTxs(blkNum, ratio, addrNum, skew)
	//
	runtime.GOMAXPROCS(runtime.NumCPU())
	//file, err := os.Open("./data/newEthTxs1.txt")
	//if err != nil {
	//	log.Panic("Read error: ", err)
	//}
	//defer file.Close()
	//r := bufio.NewReader(file)
	//var ttxs = make(map[string][]*types.Transaction)
	//for i := 0; i < blkNum; i++ {
	//	var txs []*types.Transaction
	//	for j := 0; j < 500; j++ {
	//		var tx types.Transaction
	//		txdata, err2 := r.ReadBytes('\n')
	//		if err2 != nil {
	//			if err2 == io.EOF {
	//				break
	//			}
	//			log.Panic(err2)
	//		}
	//		err2 = json.Unmarshal(txdata, &tx)
	//		if err2 != nil {
	//			log.Panic(err)
	//		}
	//		txs = append(txs, &tx)
	//	}
	//	ttxs[strconv.Itoa(i)] = txs
	//}

	var sum1 int64
	var sum2 int64
	var sum3 int64

	statedb1, _ := state.NewState(dbFile1, nil)
	statedb1.BatchCreateObjects(txs)
	for i := 0; i < 5; i++ {
		d := runTestMorphDAG(txs, float32(topk), statedb1)
		sum1 += d
	}
	fmt.Printf("Average time of MorphDAG: %.2f\n", float64(sum1)/5)

	statedb2, _ := state.NewState(dbFile2, nil)
	statedb2.BatchCreateObjects(txs)
	for i := 0; i < 5; i++ {
		d := runTestSerial(txs, statedb2)
		sum2 += d
	}
	fmt.Printf("Average time of Serial: %.2f\n", float64(sum2)/5)

	statedb3, _ := state.NewState(dbFile3, nil)
	statedb3.BatchCreateObjects(txs)
	for i := 0; i < 5; i++ {
		d := runTestNezha(txs, blkNum, statedb3)
		sum3 += d
	}
	fmt.Printf("Average time of Nezha: %.2f\n", float64(sum3)/5)
}

func runTestMorphDAG(txs map[string][]*types.Transaction, ratio float32, state *state.StateDB) int64 {
	executor := core.NewExecutor(state, 50000)
	var totalOrder []string
	for blkId := range txs {
		totalOrder = append(totalOrder, blkId)
	}
	sort.Strings(totalOrder)
	duration := executor.ProcessingTest(txs, totalOrder, ratio)
	return duration
}

func runTestSerial(txs map[string][]*types.Transaction, state *state.StateDB) int64 {
	executor := core.NewExecutor(state, 1)
	duration := executor.SerialProcessingTest(txs)
	return duration
}

func runTestNezha(txs map[string][]*types.Transaction, blkNum int, state *state.StateDB) int64 {
	start := time.Now()
	var wg sync.WaitGroup
	for _, blk := range txs {
		for _, tx := range blk {
			wg.Add(1)
			go func(t *types.Transaction) {
				defer wg.Done()
				msg := t.AsMessage()
				tp.MimicConcurrentExecution(state, msg)
			}(tx)
		}
	}
	wg.Wait()

	input := CreateNezhaRWNodes(txs)

	var mapping = make(map[string]*types.Transaction)
	for blk := range txs {
		txSets := txs[blk]
		for _, tx := range txSets {
			id := tx.String()
			mapping[id] = tx
		}
	}

	queueGraph := nezha.CreateGraph(input)
	sequence := queueGraph.QueuesSort()
	commitOrder := queueGraph.DeSS(sequence)
	tps := blkNum*500 - queueGraph.GetAbortedNums()
	fmt.Printf("Aborted txs: %d\n", queueGraph.GetAbortedNums())
	fmt.Printf("Effective tps: %d\n", tps)

	var keys []int
	for seq := range commitOrder {
		keys = append(keys, int(seq))
	}
	sort.Ints(keys)

	for _, n := range keys {
		for _, group := range commitOrder[int32(n)] {
			if len(group) > 0 {
				wg.Add(1)
				node := group[0]
				tx := mapping[node.TransInfo.ID]
				go func() {
					defer wg.Done()
					msg := tx.AsMessage()
					err := tp.ApplyMessageForNezha(state, msg)
					if err != nil {
						panic(err)
					}
				}()
			}
		}
		wg.Wait()
	}

	state.Commit()
	duration := time.Since(start)
	fmt.Printf("Time of processing transactions is: %s\n", duration)
	state.Reset()
	return int64(duration)
}

func CreateBatchTxs(blkNum, ratio int, addrNum uint64, skew float64) map[string][]*types.Transaction {
	var txs = make(map[string][]*types.Transaction)
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	z := zipf.NewZipf(r, skew, addrNum)

	selectFunc := []string{"almagate", "updateBalance", "updateSaving", "sendPayment", "writeCheck", "getBalance"}

	for i := 0; i < blkNum; i++ {
		for j := i * 500; j < (i+1)*500; j++ {
			rand.Seed(time.Now().UnixNano())
			random := rand.Intn(10)

			// read-write ratio
			var function string
			if random <= ratio {
				function = selectFunc[5]
			} else {
				random2 := rand.Intn(5)
				function = selectFunc[random2]
			}

			var duplicated = make(map[string]struct{})
			var addr1, addr2, addr3, addr4 string

			addr1 = strconv.FormatUint(z.Uint64(), 10)
			duplicated[addr1] = struct{}{}

			for {
				addr2 = strconv.FormatUint(z.Uint64(), 10)
				if _, ok := duplicated[addr2]; !ok {
					duplicated[addr2] = struct{}{}
					break
				}
			}

			for {
				addr3 = strconv.FormatUint(z.Uint64(), 10)
				if _, ok := duplicated[addr3]; !ok {
					duplicated[addr3] = struct{}{}
					break
				}
			}

			for {
				addr4 = strconv.FormatUint(z.Uint64(), 10)
				if _, ok := duplicated[addr4]; !ok {
					break
				}
			}

			payload := GeneratePayload(function, addr1, addr2, addr3, addr4)
			newTx := types.NewTransaction(0, []byte("A"), []byte("K"), payload)
			txs[strconv.Itoa(i)] = append(txs[strconv.Itoa(i)], newTx)
		}
	}

	return txs
}

func CreateTxsTest() map[int64][]*types.Transaction {
	var txs = make(map[int64][]*types.Transaction)

	var payload1 = make(map[string][]*types.RWSet)
	rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
	rw2 := &types.RWSet{Label: "w", Addr: []byte("1"), Value: 3}
	payload1["A"] = append(payload1["A"], rw1, rw2)
	tx1 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload1})

	var payload2 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("4")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("5"), Value: 3}
	payload2["C"] = append(payload2["C"], rw1)
	payload2["D"] = append(payload2["D"], rw2)
	tx2 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload2})

	txs[1] = append(txs[1], tx1, tx2)

	var payload3 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("3")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("7"), Value: 5}
	payload3["C"] = append(payload3["C"], rw1)
	payload3["B"] = append(payload3["B"], rw2)
	tx3 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload3})

	var payload4 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("5")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("7"), Value: 4}
	payload4["B"] = append(payload4["B"], rw1, rw2)
	tx4 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload4})

	txs[2] = append(txs[2], tx3, tx4)

	var payload5 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("5")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("5"), Value: 7}
	payload5["D"] = append(payload3["D"], rw1, rw2)
	tx5 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload5})

	var payload6 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("3")}
	payload6["A"] = append(payload6["A"], rw1)
	tx6 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload6})

	var payload7 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "w", Addr: []byte("1"), Value: 2}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("3"), Value: 2}
	payload7["A"] = append(payload7["A"], rw1)
	payload7["B"] = append(payload7["B"], rw2)
	tx7 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload7})

	txs[3] = append(txs[3], tx5, tx6, tx7)

	var payload8 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("2")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("2"), Value: 3}
	payload8["E"] = append(payload8["E"], rw1)
	payload8["E"] = append(payload8["E"], rw2)
	tx8 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload8})

	var payload9 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "r", Addr: []byte("4")}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("7"), Value: -10}
	payload9["E"] = append(payload9["E"], rw1)
	payload9["F"] = append(payload9["F"], rw2)
	tx9 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload9})

	var payload10 = make(map[string][]*types.RWSet)
	rw1 = &types.RWSet{Label: "w", Addr: []byte("1"), Value: 10}
	rw2 = &types.RWSet{Label: "w", Addr: []byte("1"), Value: -5}
	payload10["G"] = append(payload10["G"], rw1)
	payload10["H"] = append(payload10["H"], rw2)
	tx10 := types.NewTransaction(0, []byte("K"), []byte("A"), &types.Payload{RWSets: payload10})

	txs[4] = append(txs[4], tx8, tx9, tx10)
	return txs
}

func GeneratePayload(funcName string, addr1, addr2, addr3, addr4 string) *types.Payload {
	var payload = make(map[string][]*types.RWSet)
	switch funcName {
	// addr1 & addr3 --> savingStore, addr2 & addr4 --> checkingStore
	case "almagate":
		rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: 1000}
		payload[addr2] = append(payload[addr2], rw2)
		rw3 := &types.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr4] = append(payload[addr4], rw3)
		rw4 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr3] = append(payload[addr3], rw4)
	case "getBalance":
		rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &types.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr2] = append(payload[addr2], rw2)
	case "updateBalance":
		rw1 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr2] = append(payload[addr2], rw1)
	case "updateSaving":
		rw1 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr1] = append(payload[addr1], rw1)
	case "sendPayment":
		rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
		rw2 := &types.RWSet{Label: "w", Addr: []byte("0"), Value: -10}
		payload[addr2] = append(payload[addr2], rw1, rw2)
		rw3 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: 10}
		payload[addr4] = append(payload[addr4], rw3)
	case "writeCheck":
		rw1 := &types.RWSet{Label: "r", Addr: []byte("0")}
		payload[addr1] = append(payload[addr1], rw1)
		rw2 := &types.RWSet{Label: "iw", Addr: []byte("0"), Value: 5}
		payload[addr2] = append(payload[addr2], rw2)
	default:
		fmt.Println("Invalid inputs")
		return nil
	}
	return &types.Payload{RWSets: payload}
}

func CreateNezhaRWNodes(txs map[string][]*types.Transaction) [][]*nezha.RWNode {
	var input [][]*nezha.RWNode
	for blk := range txs {
		txSets := txs[blk]
		for _, tx := range txSets {
			var rAddr, wAddr, rValue, wValue [][]byte
			id := tx.String()
			ts := tx.Header.Timestamp
			payload := tx.Data()
			for addr := range payload.RWSets {
				rwSet := payload.RWSets[addr]
				for _, rw := range rwSet {
					if strings.Compare(rw.Label, "r") == 0 && !isExist(rAddr, []byte(addr)) {
						rAddr = append(rAddr, []byte(addr))
						rValue = append(rValue, []byte("10"))
					} else if strings.Compare(rw.Label, "w") == 0 && !isExist(wAddr, []byte(addr)) {
						wAddr = append(wAddr, []byte(addr))
						wValue = append(wValue, []byte("10"))
					} else if strings.Compare(rw.Label, "iw") == 0 && !isExist(wAddr, []byte(addr)) {
						rAddr = append(rAddr, []byte(addr))
						rValue = append(rValue, []byte("10"))
						wAddr = append(wAddr, []byte(addr))
						wValue = append(wValue, []byte("10"))
					}
				}
			}
			rwNodes := nezha.CreateRWNode(id, uint32(ts), rAddr, rValue, wAddr, wValue)
			input = append(input, rwNodes)
		}
	}
	return input
}

func isExist(data [][]byte, addr []byte) bool {
	for _, d := range data {
		if strings.Compare(string(addr), string(d)) == 0 {
			return true
		}
	}
	return false
}
