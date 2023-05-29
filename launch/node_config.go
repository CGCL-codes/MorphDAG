package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"math"
	"os"
	"strconv"
	"strings"
)

func main() {
	var local bool
	var nodeNumber int
	flag.IntVar(&nodeNumber, "n", 100, "specify the number of nodes. defaults to 100.")
	flag.BoolVar(&local, "le", true, "specify the node operating environment. defaults to true.")
	flag.Parse()

	if local {
		CreateLocalNodeAddrs2(nodeNumber)
	} else {
		CreateRemoteNodeAddrs2(nodeNumber)
	}
}

// CreateLocalNodeAddrs creates node connection file for each node (in a local environment)
func CreateLocalNodeAddrs(nodeNumber int) {
	for i := 0; i < nodeNumber; i++ {
		// each node runs one MorphDAG instance
		fileName := fmt.Sprintf("../nodefile/nodeaddrs_%s.txt", strconv.Itoa(i))
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}

		var final string
		readName := "../nodefile/config/fulladdrs_10000.txt"
		bs, err := ioutil.ReadFile(readName)
		if err != nil {
			log.Panic("Read error: ", err)
		}
		strs := strings.Split(string(bs), "\n")
		final = strs[1] + "\n"
		_, err = file.WriteString(final)
		if err != nil {
			log.Printf("error: %v\n", err)
		}

		for j := 0; j < nodeNumber; j++ {
			if j != i {
				readName := fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(j))
				bs, err := ioutil.ReadFile(readName)
				if err != nil {
					log.Panic("Read error: ", err)
				}
				strs := strings.Split(string(bs), "\n")
				final = strs[1] + "\n"

				_, err = file.WriteString(final)
				if err != nil {
					log.Printf("error: %v\n", err)
				}
			}
		}
	}

	// assume node #10000 is the tx sender
	fileName := "../nodefile/nodeaddrs_10000.txt"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	for i := 0; i < nodeNumber; i++ {
		readName := fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(i))
		bs, err := ioutil.ReadFile(readName)
		if err != nil {
			log.Panic("Read error: ", err)
		}
		strs := strings.Split(string(bs), "\n")
		ipaddr := strs[1] + "\n"

		_, err = file.WriteString(ipaddr)
		if err != nil {
			log.Printf("error: %v\n", err)
		}
	}
}

// CreateRemoteNodeAddrs creates node connection file for each node (in a distributed environment)
func CreateRemoteNodeAddrs(nodeNumber int) {
	var count int

	readIP := "../experiments/hosts.txt"
	bs, err := ioutil.ReadFile(readIP)
	if err != nil {
		log.Panic("Read error: ", err)
	}
	ipstrs := strings.Split(string(bs), "\n")

	for i := 0; i < nodeNumber; i++ {
		// each node runs two MorphDAG instances
		if i%2 == 0 {
			count++
		}
		fileName := fmt.Sprintf("../nodefile/ecs/node%s/nodeaddrs_%s.txt", strconv.Itoa(count-1), strconv.Itoa(i))
		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}

		var final string
		readName := "../nodefile/config/fulladdrs_10000.txt"
		bs, err := ioutil.ReadFile(readName)
		if err != nil {
			log.Panic("Read error: ", err)
		}
		strs := strings.Split(string(bs), "\n")

		if i == 0 || i == 1 {
			final = strs[1] + "\n"
		} else {
			lines := strings.Split(strs[0], "/")
			ip := ipstrs[0]
			final = fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s\n", ip, lines[4], lines[6])
		}

		_, err = file.WriteString(final)
		if err != nil {
			log.Printf("error: %v\n", err)
		}

		for j := 0; j < nodeNumber; j++ {
			if j != i {
				var final string
				readName := fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(j))
				bs, err := ioutil.ReadFile(readName)
				if err != nil {
					log.Panic("Read error: ", err)
				}
				strs := strings.Split(string(bs), "\n")

				if j == 2*count-1 || j == 2*count-2 {
					final = strs[1] + "\n"
				} else {
					lines := strings.Split(strs[0], "/")
					node := math.Floor(float64(j) / 2)
					ip := ipstrs[int(node)]
					final = fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s\n", ip, lines[4], lines[6])
				}

				_, err = file.WriteString(final)
				if err != nil {
					log.Printf("error: %v\n", err)
				}
			}
		}
	}

	// assume node #10000 is the tx sender (run in the instance #0 by default)
	fileName := "../nodefile/ecs/node0/nodeaddrs_10000.txt"
	file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}

	for i := 0; i < nodeNumber; i++ {
		readName := fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(i))
		bs, err := ioutil.ReadFile(readName)
		if err != nil {
			log.Panic("Read error: ", err)
		}
		strs := strings.Split(string(bs), "\n")

		if i == 0 || i == 1 {
			ipaddr := strs[1] + "\n"
			_, err = file.WriteString(ipaddr)
			if err != nil {
				log.Printf("error: %v\n", err)
			}
		} else {
			ipaddr := strs[0]
			node := math.Floor(float64(i) / 2)
			lines := strings.Split(ipaddr, "/")
			ip := ipstrs[int(node)]
			final := fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s\n", ip, lines[4], lines[6])

			_, err = file.WriteString(final)
			if err != nil {
				log.Printf("error: %v\n", err)
			}
		}
	}
}

// CreateLocalNodeAddrs2 creates node connection file for each node (using complete graph approach)
func CreateLocalNodeAddrs2(nodeNumber int) {
	nodeGraph := createNodeGraph(nodeNumber)

	for i := 0; i < nodeNumber+1; i++ {
		// each node runs one MorphDAG instances
		var fileName string
		if i == 0 {
			fileName = "../nodefile/nodeaddrs_10000.txt"
		} else {
			fileName = fmt.Sprintf("../nodefile/nodeaddrs_%s.txt", strconv.Itoa(i-1))
		}

		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}

		for n := range nodeGraph[i] {
			var readName string
			if n == 0 {
				readName = "../nodefile/config/fulladdrs_10000.txt"
			} else {
				readName = fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(n-1))
			}
			bs, err := ioutil.ReadFile(readName)
			if err != nil {
				log.Panic("Read error: ", err)
			}
			strs := strings.Split(string(bs), "\n")
			ipaddr := strs[1] + "\n"

			_, err = file.WriteString(ipaddr)
			if err != nil {
				log.Printf("error: %v\n", err)
			}
		}
	}
}

// CreateRemoteNodeAddrs2 creates node connection file for each node (using complete graph approach)
func CreateRemoteNodeAddrs2(nodeNumber int) {
	var count int
	nodeGraph := createNodeGraph(nodeNumber)

	readIP := "../experiments/hosts.txt"
	bs, err := ioutil.ReadFile(readIP)
	if err != nil {
		log.Panic("Read error: ", err)
	}
	ipstrs := strings.Split(string(bs), "\n")

	for i := 0; i < nodeNumber+1; i++ {
		// each node runs two MorphDAG instances
		var fileName string
		if i == 0 {
			fileName = "../nodefile/ecs/node0/nodeaddrs_10000.txt"
		} else {
			if i%2 == 1 {
				count++
			}
			fileName = fmt.Sprintf("../nodefile/ecs/node%s/nodeaddrs_%s.txt", strconv.Itoa(count-1), strconv.Itoa(i-1))
		}

		file, err := os.OpenFile(fileName, os.O_RDWR|os.O_CREATE|os.O_TRUNC|os.O_APPEND, 0666)
		if err != nil {
			fmt.Printf("error: %v\n", err)
		}

		for n := range nodeGraph[i] {
			var readName, ip string
			if n == 0 {
				readName = "../nodefile/config/fulladdrs_10000.txt"
			} else {
				readName = fmt.Sprintf("../nodefile/config/fulladdrs_%s.txt", strconv.Itoa(n-1))
			}

			bs, err := ioutil.ReadFile(readName)
			if err != nil {
				log.Panic("Read error: ", err)
			}
			strs := strings.Split(string(bs), "\n")
			ipaddr := strs[0]
			lines := strings.Split(ipaddr, "/")

			if n == 0 {
				ip = ipstrs[0]
			} else {
				node := math.Ceil(float64(n)/2) - 1
				ip = ipstrs[int(node)]
			}
			final := fmt.Sprintf("/ip4/%s/tcp/%s/p2p/%s\n", ip, lines[4], lines[6])

			_, err = file.WriteString(final)
			if err != nil {
				log.Printf("error: %v\n", err)
			}
		}
	}
}

func createNodeGraph(nodeNumber int) map[int]map[int]struct{} {
	var nodeGraph = make(map[int]map[int]struct{})
	avgEdges := math.Floor(float64(nodeNumber-1) / 2)

	for i := 0; i < nodeNumber+1; i++ {
		var counter int
		nodeGraph[i] = make(map[int]struct{})
		for j := 0; j < nodeNumber+1; j++ {
			if i < int(math.Floor(float64(nodeNumber+1)/2)) && counter == int(avgEdges) {
				break
			}

			if j != i {
				if tinyGraph, ok := nodeGraph[j]; ok {
					if _, ok2 := tinyGraph[i]; !ok2 {
						nodeGraph[i][j] = struct{}{}
						counter++
					}
				} else {
					nodeGraph[i][j] = struct{}{}
					counter++
				}
			}

		}
	}

	return nodeGraph
}
