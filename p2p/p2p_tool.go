package p2p

import (
	"MorphDAG/config"
	"MorphDAG/utils"
	"bufio"
	"context"
	"fmt"
	"github.com/libp2p/go-libp2p"
	"github.com/libp2p/go-libp2p/core/crypto"
	"github.com/libp2p/go-libp2p/core/host"
	"github.com/libp2p/go-libp2p/core/network"
	"github.com/libp2p/go-libp2p/core/peer"
	"github.com/libp2p/go-libp2p/core/peerstore"
	"github.com/libp2p/go-libp2p/core/protocol"
	"github.com/multiformats/go-multiaddr"
	"log"
	"os"
	"sync"
)

// NetworkDealer deals with the node connection and communication
type NetworkDealer struct {
	//txConnPool       map[string]*conn
	//blkConnPool      map[string]*conn
	//sigConnPool      map[string]*conn
	connPool       map[string]*conn
	connPoolLock   sync.Mutex
	syncLock       sync.Mutex
	txChannel      chan MsgWithType
	blkChannel     chan MsgWithType
	payloadChannel chan MsgWithType
	//signalChannel chan MsgWithType
	SignalHandler func(request []byte)

	Host host.Host
}

// MsgWithType a message format with a type
type MsgWithType struct {
	Msg  []byte
	Type string
}

// conn maintains network connection information
type conn struct {
	dest   string
	writer *bufio.Writer
}

//// maintain a list of bootstrap peers
//var defaultBootstrapPeers []multiaddr.Multiaddr
//
//func init() {
//	for _, s := range []string{
//		"/dnsaddr/bootstrap.libp2p.io/p2p/QmNnooDu7bfjPFoTZYxMNLWUQJyrVwtbZg5gBMjTezGAJN",
//		"/dnsaddr/bootstrap.libp2p.io/p2p/QmQCU2EcMqAqQPR2i9bChDtGNJchTbq5TbXJJ16u19uLTa",
//		"/dnsaddr/bootstrap.libp2p.io/p2p/QmbLHAnMoJPWSCR5Zhtx6BHJX9KiKNN6tpvbUcqanj75Nb",
//		"/dnsaddr/bootstrap.libp2p.io/p2p/QmcZf59bWwK5XFi76CZX8cbJ4BhTzzA3gU1ZjYZcYW3dwt",
//		"/ip4/104.131.131.82/tcp/4001/p2p/QmaCpDMGvV2BGHeYERUEnRQAwe3N8SzbUtfsmvsqQLuvuJ",
//	} {
//		ma, err := multiaddr.NewMultiaddr(s)
//		if err != nil {
//			panic(err)
//		}
//		defaultBootstrapPeers = append(defaultBootstrapPeers, ma)
//	}
//}

// MakeHost creates a LibP2P host listening on the given port
func MakeHost(listenPort int, pk crypto.PrivKey, addrsPath string) (host.Host, error) {
	sourceMultiAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ip4/0.0.0.0/tcp/%d", listenPort))

	basicHost, err := libp2p.New(
		libp2p.Identity(pk),
		libp2p.ListenAddrs(sourceMultiAddr),
		//libp2p.ForceReachabilityPublic(),
		//libp2p.NATPortMap(),
		//libp2p.EnableRelay(),
	)
	if err != nil {
		return nil, err
	}

	hostAddr, _ := multiaddr.NewMultiaddr(fmt.Sprintf("/ipfs/%s", basicHost.ID().String()))
	if err != nil {
		return nil, err
	}

	var fullAddrs []string
	for i, addr := range basicHost.Addrs() {
		fullAddr := addr.Encapsulate(hostAddr).String()
		log.Printf("The %d th fullAddr: %s\n", i, fullAddr)
		fullAddrs = append(fullAddrs, fullAddr)
	}

	if utils.FileExists(addrsPath) {
		log.Printf("File %s already exists!", addrsPath)
	} else {
		f, err := os.Create(addrsPath)
		if err != nil {
			return nil, err
		}
		defer f.Close()
		for _, addr := range fullAddrs {
			_, err = f.WriteString(addr + "\n")
			if err != nil {
				return nil, err
			}
		}
	}

	return basicHost, nil
}

// OpenP2PListen opens a listening for connection
func (n *NetworkDealer) OpenP2PListen() {
	listenStream := func(s network.Stream) {
		dest := s.Conn().RemotePeer().String()
		log.Println("Received a connection from ", dest)
		writer := bufio.NewWriter(s)
		c := &conn{
			dest:   dest,
			writer: writer,
		}
		n.connPool[dest] = c
		r := bufio.NewReader(s)
		go n.HandleConn(r)
	}

	n.Host.SetStreamHandler(protocol.ID("morph"), listenStream)
	//listenStream2 := func(s network.Stream) {
	//	dest := s.Conn().RemotePeer().String()
	//	log.Println("Received a block connection from ", dest)
	//	//blkWriter := bufio.NewWriter(s)
	//	//c := &conn{
	//	//	dest:   dest,
	//	//	writer: blkWriter,
	//	//}
	//	//n.blkConnPool[dest] = c
	//	go n.HandleConn(bufio.NewReader(s))
	//}
	//
	//listenStream3 := func(s network.Stream) {
	//	dest := s.Conn().RemotePeer().String()
	//	log.Println("Received a signal connection from ", dest)
	//	//signalWriter := bufio.NewWriter(s)
	//	//c := &conn{
	//	//	dest:   dest,
	//	//	writer: signalWriter,
	//	//}
	//	//n.sigConnPool[dest] = c
	//	go n.HandleConn(bufio.NewReader(s))
	//}
	//n.Host.SetStreamHandler(protocol.ID(pid2), listenStream2)
	//n.Host.SetStreamHandler(protocol.ID(pid3), listenStream3)
}

// HandleConn reads a transaction or a block from P2P connection and deals with it
func (n *NetworkDealer) HandleConn(reader *bufio.Reader) {
	for {
		str, err := reader.ReadString('\n')
		if err != nil {
			log.Panic(err)
		}

		if len(str) == 0 {
			return
		}

		bytes := []byte(str)
		command := bytesToCommand(bytes[:config.CommandLength])

		msgWithType := MsgWithType{
			Msg:  bytes,
			Type: command,
		}

		if command == "tx" {
			select {
			case n.txChannel <- msgWithType:
			}
		} else if command == "block" {
			select {
			case n.blkChannel <- msgWithType:
			}
		} else if command == "payload" {
			select {
			case n.payloadChannel <- msgWithType:
			}
		} else if command == "sync" {
			n.SignalHandler(bytes)
		}
	}
}

// ConnectPeer connects to a specific peer
func (n *NetworkDealer) ConnectPeer(dest string) error {
	maddr, err := multiaddr.NewMultiaddr(dest)
	if err != nil {
		return err
	}

	info, err := peer.AddrInfoFromP2pAddr(maddr)
	if err != nil {
		return err
	}

	n.Host.Peerstore().AddAddrs(info.ID, info.Addrs, peerstore.PermanentAddrTTL)

	// start streams with the dest peer
	s, err := n.Host.NewStream(context.Background(), info.ID, protocol.ID("morph"))
	if err != nil {
		return err
	}
	writer := bufio.NewWriter(s)

	c := &conn{
		dest:   info.ID.String(),
		writer: writer,
	}
	n.connPool[info.ID.String()] = c

	go n.HandleConn(bufio.NewReader(s))

	//if sender {
	//	s1, err := n.Host.NewStream(context.Background(), info.ID, protocol.ID(pid1))
	//	if err != nil {
	//		return err
	//	}
	//	txWriter := bufio.NewWriter(s1)
	//	c1 := &conn{
	//		dest:   info.ID.String(),
	//		writer: txWriter,
	//	}
	//	n.txConnPool[info.ID.String()] = c1
	//	// go n.HandleConn(bufio.NewReader(s1))
	//	return nil
	//}
	//
	//s2, err := n.Host.NewStream(context.Background(), info.ID, protocol.ID(pid2))
	//if err != nil {
	//	return err
	//}
	//blkWriter := bufio.NewWriter(s2)
	//c2 := &conn{
	//	dest:   info.ID.String(),
	//	writer: blkWriter,
	//}
	//n.blkConnPool[info.ID.String()] = c2
	////go n.HandleConn(bufio.NewReader(s2))
	//
	//s3, err := n.Host.NewStream(context.Background(), info.ID, protocol.ID(pid3))
	//if err != nil {
	//	return err
	//}
	//signalWriter := bufio.NewWriter(s3)
	//c3 := &conn{
	//	dest:   info.ID.String(),
	//	writer: signalWriter,
	//}
	//n.sigConnPool[info.ID.String()] = c3
	//go n.HandleConn(bufio.NewReader(s3))
	return nil
}

// SyncMsg broadcasts a message to all network nodes
func (n *NetworkDealer) SyncMsg(msg []byte) error {
	//var connPool map[string]*conn
	//switch msgType {
	//case "tx":
	//	connPool = n.txConnPool
	//case "block":
	//	connPool = n.blkConnPool
	//case "signal":
	//	connPool = n.sigConnPool
	//}
	n.syncLock.Lock()
	defer n.syncLock.Unlock()
	var wg sync.WaitGroup

	for _, con := range n.connPool {
		wg.Add(1)
		c := con
		go func() {
			defer wg.Done()
			err := n.sendMsg(msg, c.dest)
			if err != nil {
				// reconnect the node and resend
				success := false
				for i := 2; i > 0; i-- {
					log.Println("fail to send msg to dest: ", c.dest, "try sending again")
					err2 := n.sendMsg(msg, c.dest)
					if err2 == nil {
						success = true
						break
					}
				}
				if !success {
					log.Println("fail to send msg to dest after retry: ", c.dest)
				}
			}
		}()
	}
	wg.Wait()

	return nil
}

// sendMsg sends a message to a specific node
func (n *NetworkDealer) sendMsg(msg []byte, dest string) error {
	n.connPoolLock.Lock()
	defer n.connPoolLock.Unlock()
	c, ok := n.connPool[dest]
	if !ok {
		err := n.ConnectPeer(c.dest)
		if err != nil {
			return err
		}
	}

	_, err := c.writer.WriteString(fmt.Sprintf("%s\n", string(msg)))
	if err != nil {
		return err
	}
	err = c.writer.Flush()
	if err != nil {
		return err
	}
	return nil
}

// StartPeer launches a peer
func StartPeer(port int, prvkey crypto.PrivKey, addrsPath string) (*NetworkDealer, error) {
	n, err := NewNetworkDealer(port, prvkey, addrsPath)
	if err != nil {
		return nil, err
	}
	n.OpenP2PListen()
	log.Printf("Open %d port for p2p listening", port)
	return n, nil
}

// NewNetworkDealer creates a new instance of network dealer
func NewNetworkDealer(port int, prvkey crypto.PrivKey, addrsPath string) (*NetworkDealer, error) {
	h, err := MakeHost(port, prvkey, addrsPath)
	if err != nil {
		return nil, err
	}

	n := &NetworkDealer{
		connPool:       make(map[string]*conn),
		txChannel:      make(chan MsgWithType, 50000),
		blkChannel:     make(chan MsgWithType, 10000),
		payloadChannel: make(chan MsgWithType, 10000),
		Host:           h,
	}

	return n, nil
}

func (n *NetworkDealer) ExtractTx() chan MsgWithType {
	return n.txChannel
}

func (n *NetworkDealer) ExtractBlk() chan MsgWithType {
	return n.blkChannel
}

func (n *NetworkDealer) ExtractPayload() chan MsgWithType {
	return n.payloadChannel
}
