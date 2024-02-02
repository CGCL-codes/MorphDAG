package p2p

import (
	"MorphDAG/core/types"
	"encoding/json"
	"errors"
	"fmt"
	"time"
)

type RPCServer struct {
	RPC     *Server
	Network *NetworkDealer
}

type SendBatchTxsCmd struct {
	Txs      []*types.Transaction
	Interval float64
}

type SendBatchTxsReply struct {
	Msg string
}

type GetBalanceCmd struct {
	Address string
}

type GetBalanceReply struct {
	Msg string
}

type ObserveCmd struct{}

type ObserveReply struct {
	Completed map[string][]*types.Transaction
}

func StartRPCServer(server *Server, dealer *NetworkDealer) *RPCServer {
	return &RPCServer{server, dealer}
}

func (server *RPCServer) SendBatchTxs(cmd SendBatchTxsCmd, reply *SendBatchTxsReply) error {
	txs := cmd.Txs
	for _, t := range txs {
		// serialize tx data and broadcast to the network
		txData := tx{server.RPC.NodeID, *t}
		payload, _ := json.Marshal(txData)
		request := append(commandToBytes("tx"), payload...)
		err := server.Network.SyncMsg(request)
		if err != nil {
			return err
		}
		for k := 0; k < int(cmd.Interval); k++ {
			time.Sleep(time.Millisecond)
		}
	}

	reply.Msg = "Batch of TXs sent success!"
	return nil
}

func (server *RPCServer) GetBalance(cmd GetBalanceCmd, reply *GetBalanceReply) error {
	stateDB := server.RPC.StateDB
	addr := []byte(cmd.Address)
	bal, err := stateDB.GetBalance(addr)
	if err != nil {
		return err
	}
	reply.Msg = fmt.Sprintf("Balance of '%s': %x\n", cmd.Address, bal)
	return nil
}

func (server *RPCServer) ObserveLatency(cmd ObserveCmd, reply *ObserveReply) error {
	reply.Completed = server.RPC.CompletedQueue
	if len(reply.Completed) == 0 {
		err := errors.New("empty queue")
		return err
	}
	return nil
}
