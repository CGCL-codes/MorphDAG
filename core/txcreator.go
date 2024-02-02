package core

import (
	"MorphDAG/core/types"
	"fmt"
	"github.com/chinuy/zipf"
	"math/rand"
	"strconv"
	"time"
)

var selectFunc = []string{"almagate", "updateBalance", "updateSaving", "sendPayment", "writeCheck", "getBalance"}

// CreateMimicWorkload creates a transaction using smallbank benchmark
func CreateMimicWorkload(ratio int, z *zipf.Zipf) *types.Payload {
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

	payload := generatePayload(function, addr1, addr2, addr3, addr4)

	return payload
}

func generatePayload(funcName string, addr1, addr2, addr3, addr4 string) *types.Payload {
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
