package cli

import (
	"fmt"
	"github.com/urfave/cli/v2"
	"log"
	"os"
)

type CMDServer struct {
	P2PPort    int
	RPCPort    int
	Cycles     int
	NodeNumber int
	Large      int
	NodeFile   string
	Pid        string
	//Pid2          string
	//Pid3          string
	FullAddrsPath string
	PKFile        string
	TxSender      bool
	Observer      bool
	Config        bool
}

func (cmd *CMDServer) Run(nodeID string) {
	app := &cli.App{
		Flags: []cli.Flag{
			&cli.IntFlag{
				Name:        "p2pport",
				Aliases:     []string{"pp"},
				Usage:       "P2P Port to listen to",
				Value:       7000,
				Destination: &cmd.P2PPort,
			},
			&cli.IntFlag{
				Name:        "rpcport",
				Aliases:     []string{"rp"},
				Usage:       "RPC port to listen to",
				Value:       5000,
				Destination: &cmd.RPCPort,
			},
			&cli.IntFlag{
				Name:        "cycles",
				Aliases:     []string{"c"},
				Usage:       "Number of cycles to run",
				Value:       50,
				Destination: &cmd.Cycles,
			},
			&cli.IntFlag{
				Name:        "number",
				Aliases:     []string{"n"},
				Usage:       "Number of nodes",
				Value:       100,
				Destination: &cmd.NodeNumber,
			},
			&cli.IntFlag{
				Name:        "loadfile",
				Aliases:     []string{"l"},
				Usage:       "Indicate which tx data file to use",
				Value:       1,
				Destination: &cmd.Large,
			},
			&cli.StringFlag{
				Name:        "nodefile",
				Aliases:     []string{"nf"},
				Usage:       "Path of a file storing the destination node addresses for connecting",
				Value:       fmt.Sprintf("../nodefile/nodeaddrs_%s.txt", nodeID),
				Destination: &cmd.NodeFile,
			},
			&cli.StringFlag{
				Name:        "pid",
				Aliases:     []string{"p"},
				Value:       "MorphDAG",
				Usage:       "pid to identify a network protocol",
				Destination: &cmd.Pid,
			},
			//&cli.StringFlag{
			//	Name:        "pid2",
			//	Aliases:     []string{"p2"},
			//	Value:       "MorphDAG-Block",
			//	Usage:       "pid to identify a network protocol",
			//	Destination: &cmd.Pid2,
			//},
			//&cli.StringFlag{
			//	Name:        "pid3",
			//	Aliases:     []string{"p3"},
			//	Value:       "MorphDAG-Signal",
			//	Usage:       "pid to identify a network protocol",
			//	Destination: &cmd.Pid3,
			//},
			&cli.StringFlag{
				Name:        "fulladdrs",
				Aliases:     []string{"f"},
				Value:       fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", nodeID),
				Usage:       "Path of a file storing the full node addresses for listening",
				Destination: &cmd.FullAddrsPath,
			},
			&cli.StringFlag{
				Name:        "pkfile",
				Aliases:     []string{"pk"},
				Value:       fmt.Sprintf("../nodefile/config/MorphDAG_%s.pk", nodeID),
				Usage:       "Path of a file storing the private key",
				Destination: &cmd.PKFile,
			},
			&cli.BoolFlag{
				Name:        "sender",
				Aliases:     []string{"sd"},
				Value:       false,
				Usage:       "Identifier to identify the transaction sender",
				Destination: &cmd.TxSender,
			},
			&cli.BoolFlag{
				Name:        "observer",
				Aliases:     []string{"ob"},
				Value:       false,
				Usage:       "Identifier to identify the system performance observer",
				Destination: &cmd.Observer,
			},
			&cli.BoolFlag{
				Name:        "config",
				Aliases:     []string{"cf"},
				Value:       false,
				Usage:       "Identifier to identify whether it is in the configuration phase",
				Destination: &cmd.Config,
			},
		},
	}

	err := app.Run(os.Args)
	if err != nil {
		log.Panic(err)
	}
}
