package cli

import (
	"MorphDAG/config"
	"MorphDAG/core/types"
	"MorphDAG/p2p"
	"bufio"
	"encoding/json"
	"fmt"
	"github.com/urfave/cli/v2"
	"io"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"time"
)

type CMDClient struct {
	rpcPort int
}

func (cmd *CMDClient) Run() {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:        "rpcport",
				Usage:       "RPC port to connect to",
				Destination: &cmd.rpcPort,
			},
		},
		Commands: []*cli.Command{
			{
				Name:      "sendtxs",
				Aliases:   []string{"sts"},
				Usage:     "Send batch of transactions to the network",
				UsageText: "sendtxs -cyc CYCLES -large LARGE (0 or 1)",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "cyc",
						Aliases: []string{"c"},
					},
					&cli.IntFlag{
						Name:    "large",
						Aliases: []string{"l"},
					},
				},
				Action: func(context *cli.Context) error {
					cycles := context.Int("cyc")
					largeLoads := context.Int("large")
					cmd.SendBatchTxs(cycles, largeLoads)
					return nil
				},
			},
			{
				Name:      "getbalance",
				Aliases:   []string{"gb"},
				Usage:     "Get balance of a specific address ADDRESS",
				UsageText: "getbalance -address ADDRESS",
				Flags: []cli.Flag{
					&cli.StringFlag{
						Name:    "address",
						Aliases: []string{"a"},
					},
				},
				Action: func(context *cli.Context) error {
					address := context.String("address")
					cmd.GetBalance(address)
					return nil
				},
			},
			{
				Name:      "observe",
				Aliases:   []string{"ob"},
				Usage:     "Observe the transaction latency and throughput",
				UsageText: "observe -cyc CYCLES",
				Flags: []cli.Flag{
					&cli.IntFlag{
						Name:    "cyc",
						Aliases: []string{"c"},
					},
				},
				Action: func(context *cli.Context) error {
					cycles := context.Int("cyc")
					for i := 0; i < cycles; i++ {
						time.Sleep(time.Second * 5)
						cmd.GetLatency()
					}
					return nil
				},
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Panic(err)
	}
}

func (cmd *CMDClient) SendBatchTxs(cycles, largeLoads int) {
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

	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(cmd.rpcPort))
	if err != nil {
		log.Panic("Connection error: ", err)
	}

	for i := 0; i < cycles; i++ {
		var txs []*types.Transaction
		load := loads[i]
		interval := 1000 * (8 / float64(load))

		for j := 0; j < load; j++ {
			var tx types.Transaction
			txdata, err2 := r.ReadBytes('\n')
			if err2 != nil {
				if err2 == io.EOF {
					break
				}
				log.Panic(err2)
			}
			err2 = json.Unmarshal(txdata, &tx)
			if err2 != nil {
				log.Panic(err)
			}
			txs = append(txs, &tx)
		}

		sRequest := p2p.SendBatchTxsCmd{Txs: txs, Interval: interval}
		sReply := p2p.SendBatchTxsReply{}

		err = client.Call("RPCServer.SendBatchTxs", sRequest, &sReply)
		if err != nil {
			log.Panic("Send error: ", err)
		} else {
			fmt.Println(sReply.Msg)
		}
	}
}

func (cmd *CMDClient) GetBalance(address string) {
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(cmd.rpcPort))
	if err != nil {
		log.Panic("Connection error: ", err)
	}

	gRequest := p2p.GetBalanceCmd{Address: address}
	gReply := p2p.GetBalanceReply{}

	err = client.Call("RPCServer.GetBalance", gRequest, &gReply)
	if err != nil {
		log.Panic("Query error: ", err)
	} else {
		fmt.Println(gReply.Msg)
	}
}

func (cmd *CMDClient) GetLatency() {
	client, err := rpc.DialHTTP("tcp", "localhost:"+strconv.Itoa(cmd.rpcPort))
	if err != nil {
		log.Panic("Connection error: ", err)
	}

	gRequest := p2p.ObserveCmd{}
	gReply := p2p.ObserveReply{}

	for {
		err2 := client.Call("RPCServer.ObserveLatency", gRequest, &gReply)
		if err2 == nil {
			break
		}
	}

	txs := gReply.Completed
	getAppendLatency(txs)
	getOverallLatency(txs)
}

// getAppendLatency calculates the latency and throughput of transactions being appended to the DAG
func getAppendLatency(txs map[string][]*types.Transaction) {
	file, err := os.OpenFile(config.ExpResult1, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	num := 0
	sum := 0
	for _, ts := range txs {
		for _, t := range ts {
			num++
			lat := t.GetEnd1() - t.GetStart()
			sum += int(lat)
		}
	}

	avgLat := float64(sum) / float64(num)
	avgTPS := float64(num) / avgLat
	avgMbps := ((0.83 * float64(num*8)) / 1024) / avgLat
	contents := fmt.Sprintf("Time: %d, number of blocks: %d, average latency: %.2f, average tps: %.2f, average Mbps: %.2f \n",
		time.Now().Unix(), len(txs), avgLat, avgTPS, avgMbps)
	_, err = file.WriteString(contents)
	if err != nil {
		log.Printf("error: %v\n", err)
	}
}

// getOverallLatency calculates the latency and throughput of state persistence
func getOverallLatency(txs map[string][]*types.Transaction) {
	file, err := os.OpenFile(config.ExpResult2, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	num := 0
	sum := 0
	for _, ts := range txs {
		for _, t := range ts {
			num++
			lat := t.GetEnd2() - t.GetStart()
			sum += int(lat)
		}
	}

	avgLat := float64(sum) / float64(num)
	avgTPS := float64(num) / avgLat
	avgMbps := ((0.83 * float64(num*8)) / 1024) / avgLat
	contents := fmt.Sprintf("Time: %d, number of blocks: %d, average latency: %.2f, average tps: %.2f, average Mbps: %.2f \n",
		time.Now().Unix(), len(txs), avgLat, avgTPS, avgMbps)
	_, err = file.WriteString(contents)
	if err != nil {
		log.Printf("error: %v\n", err)
	}
}
