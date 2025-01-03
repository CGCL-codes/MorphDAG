package main

import (
	"MorphDAG/cli"
	"MorphDAG/p2p"
	"MorphDAG/utils"
	"fmt"
	"log"
	"os"
	"time"
)

var nodeID string
var cmd cli.CMDServer
var dagServer *p2p.Server

func init() {
	nodeID = os.Getenv("NODE_ID")
	if nodeID == "" {
		fmt.Printf("NODE_ID env. var is not set!")
		os.Exit(1)
	}
	cmd.Run(nodeID)
}

// start the p2p connection and the rpc server
func init() {
	sk, err := utils.GetPrivateKey(cmd.PKFile)
	if err != nil {
		log.Fatal("Opening file error: ", err)
	}

	if cmd.Config {
		_, err = p2p.MakeHost(cmd.P2PPort, sk, cmd.FullAddrsPath)
		if err != nil {
			log.Fatal("Fail to build P2P host: ", err)
		}
	} else {
		// initialize DAG blockchain
		dealer, err := p2p.StartPeer(cmd.P2PPort, sk, cmd.FullAddrsPath)
		if err != nil {
			log.Fatal("Fail to build P2P dealer: ", err)
		}

		dagServer = p2p.InitializeServer(nodeID, cmd.NodeNumber, dealer, cmd.TxSender)
		if !cmd.TxSender {
			dagServer.CreateDAG()
		}

		// start the rpc server
		//rpcServer := p2p.StartRPCServer(dagServer, dealer)
		//err = rpc.Register(rpcServer)
		//if err != nil {
		//	log.Fatal("Wrong format of service!", err)
		//}
		//
		//rpc.HandleHTTP()
		//
		//listener, err := net.Listen("tcp", "localhost:"+strconv.Itoa(cmd.RPCPort))
		//if err != nil {
		//	log.Fatal("Listen error: ", err)
		//}
		//
		//log.Printf("RPC server listening on port %d", cmd.RPCPort)
		//go http.Serve(listener, nil)

		// start the p2p connection
		dagServer.Network.SignalHandler = dagServer.ProcessSyncSignal

		time.Sleep(20 * time.Second)

		ipaddrs, err := utils.ReadStrings(cmd.NodeFile)
		if err != nil {
			log.Fatal("Fail to read ip addresses: ", err)
		}

		for _, ip := range ipaddrs {
			err = dagServer.Network.ConnectPeer(ip)
			if err != nil {
				log.Fatal("Fail to connect: ", err)
			}
		}
		fmt.Printf("Successfully connect to the connected peers, %s th node\n", nodeID)

		time.Sleep(20 * time.Second)
	}
}

// start to run MorphDAG instance
func main() {
	if !cmd.Config {
		if cmd.TxSender {
			time.Sleep(4 * time.Second)
			go dagServer.SendTxsForLoop(cmd.Cycles, cmd.Large)
			select {}
		} else {
			go dagServer.HandleTxForever()
			go dagServer.HandleBlkForever()
			go dagServer.HandlePayloadForever()
			if cmd.Observer {
				go dagServer.ObserveSystemTPS(cmd.Cycles)
			}
			dagServer.Run(cmd.Cycles)
		}
	}
}
