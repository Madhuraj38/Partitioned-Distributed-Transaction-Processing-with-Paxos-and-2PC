package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

type Transaction struct {
	SeqId       int
	Sender      int
	Receiver    int
	Amount      int
	CommitIndex int
	Status      string
}

type TransactionSet struct {
	SetNumber      int
	Transactions   []Transaction
	LiveServers    []int
	ContactServers []int
}

type Coordinator struct {
	mu           sync.Mutex
	preparedTxns map[string]map[int]bool
}

var ClusterAddresses = map[int][]string{
	0: {"localhost:1111", "localhost:2222", "localhost:3333"},
	1: {"localhost:4444", "localhost:5555", "localhost:6666"},
	2: {"localhost:7777", "localhost:8888", "localhost:9999"},
}
var clusterToServer = make(map[int]int)
var mapLock sync.Mutex
var startTimes = make(map[string]time.Time)
var elapsedTimeMs = make(map[string]int64)

var logger *log.Logger

func initLogger() {
	file, err := os.OpenFile("twophasecommit.log", os.O_TRUNC|os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		log.Fatalf("Failed to open log file: %v", err)
	}
	logger = log.New(file, "LOG: ", log.Ldate|log.Ltime|log.Lshortfile)
}

func ParseTransactionsFromCSV(filename string) ([]TransactionSet, error) {
	file, err := os.Open(filename)
	if err != nil {
		return nil, err
	}
	defer file.Close()

	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, err
	}

	var transactionSets []TransactionSet
	var currentSet TransactionSet

	for i, record := range records {
		if i == 0 {
			record[0] = strings.TrimPrefix(record[0], "\ufeff")
		}
		if record[0] != "" {
			setNumber, _ := strconv.Atoi(record[0])
			if setNumber != currentSet.SetNumber {
				if currentSet.SetNumber != 0 {
					transactionSets = append(transactionSets, currentSet)
				}
				currentSet = TransactionSet{SetNumber: setNumber, LiveServers: parseLiveServers(record[2]), ContactServers: parseLiveServers(record[3])}
			}
		}

		tx := parseTransaction(record[1])
		currentSet.Transactions = append(currentSet.Transactions, tx)

	}

	if currentSet.SetNumber != 0 {
		transactionSets = append(transactionSets, currentSet)
	}

	return transactionSets, nil
}

func parseTransaction(input string) Transaction {
	input = strings.Trim(input, "()")
	parts := strings.Split(input, ",")
	amount, _ := strconv.Atoi(strings.TrimSpace(parts[2]))
	sender, _ := strconv.Atoi(strings.TrimSpace(parts[0]))
	receiver, _ := strconv.Atoi(strings.TrimSpace(parts[1]))
	return Transaction{
		Sender:   sender,
		Receiver: receiver,
		Amount:   amount,
	}
}

func parseLiveServers(input string) []int {
	input = strings.Trim(input, "[]")
	serverStrings := strings.Split(input, ",")
	var servers []int

	for _, server := range serverStrings {
		server = strings.TrimSpace(server)
		if len(server) > 1 && server[0] == 'S' {
			if num, err := strconv.Atoi(server[1:]); err == nil {
				servers = append(servers, num-1)
			}
		}
	}

	return servers
}

func MakeCoordinator(port string) *Coordinator {
	coordinator := &Coordinator{
		preparedTxns: make(map[string]map[int]bool),
	}

	rpcs := rpc.NewServer()
	err := rpcs.Register(coordinator)
	if err != nil {
		log.Fatalf("Failed to register Coordinator: %v", err)
	}

	listener, err := net.Listen("tcp", port)
	if err != nil {
		log.Fatalf("Failed to start Coordinator listener: %v", err)
	}

	go func() {
		for {
			conn, err := listener.Accept()
			if err == nil {
				go rpcs.ServeConn(conn)
			} else {
				logger.Printf("Coordinator accept error: %v", err)
			}
		}
	}()

	logger.Printf("Coordinator started and listening on %s", port)
	return coordinator
}

func ShardMapping(id int) string {
	if id >= 1 && id <= 1000 {
		return "D1"
	} else if id >= 1001 && id <= 2000 {
		return "D2"
	} else {
		return "D3"
	}
}

func getClusterID(shardID string) int {
	switch shardID {
	case "D1":
		return 0
	case "D2":
		return 1
	case "D3":
		return 2
	default:
		return -1
	}
}

func mapContactServersToClusters(contactServers []int) map[int]int {
	clusterToServer[0] = contactServers[0]
	clusterToServer[1] = contactServers[1]
	clusterToServer[2] = contactServers[2]

	return clusterToServer
}

func sendCommitOrAbort(clusters []int, tx Transaction, commit bool) {
	var wg sync.WaitGroup
	action := "Abort"
	if commit {
		action = "Commit"
	}

	for _, clusterID := range clusters {
		for _, peer := range ClusterAddresses[clusterID] {
			wg.Add(1)
			go func(peer string, action string) {
				defer wg.Done()
				logger.Printf("Sending %s request for transaction %v to peer %s", action, tx, peer)
				var response bool
				call(peer, "Paxos."+action+"Transaction", tx, &response)
			}(peer, action)
		}
	}

	wg.Wait()
	key := fmt.Sprintf("%d#%d#%d", tx.SeqId, tx.Sender, tx.Receiver)
	mapLock.Lock()
	if startTime, exists := startTimes[key]; exists {
		elapsedTimeMs[key] = time.Now().Sub(startTime).Milliseconds()
	}
	mapLock.Unlock()
	logger.Printf("%s requests completed for transaction %v", action, tx)
}

func (c *Coordinator) ReceivePrepareResponse(request struct {
	Tx        Transaction
	ClusterID int
	Status    bool
}, reply *bool) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	requestID := fmt.Sprintf("%d#%d#%d", request.Tx.SeqId, request.Tx.Sender, request.Tx.Receiver)
	if ShardMapping(request.Tx.Sender) == ShardMapping(request.Tx.Receiver) {
		mapLock.Lock()
		if startTime, exists := startTimes[requestID]; exists {
			elapsedTimeMs[requestID] = time.Now().Sub(startTime).Milliseconds()
		}
		mapLock.Unlock()
		*reply = true
		return nil
	}

	if _, exists := c.preparedTxns[requestID]; !exists {
		c.preparedTxns[requestID] = make(map[int]bool)
	}
	c.preparedTxns[requestID][request.ClusterID] = request.Status

	logger.Printf("Coordinator received prepare response for transaction %v from cluster %d: %v",
		request.Tx, request.ClusterID, request.Status)

	if len(c.preparedTxns[requestID]) == 2 {
		successfulClusters := []int{}

		for clusterID, prepared := range c.preparedTxns[requestID] {
			if prepared {
				successfulClusters = append(successfulClusters, clusterID)
			}
		}
		time.Sleep(100 * time.Millisecond)
		if len(successfulClusters) == 2 {
			logger.Printf("Transaction %v prepared successfully in both clusters. Sending Commit.", request.Tx.SeqId)
			sendCommitOrAbort(successfulClusters, request.Tx, true)
		} else if len(successfulClusters) == 1 {
			logger.Printf("Transaction %v failed to prepare in one cluster. Sending Abort to cluster %d.", request.Tx.SeqId, successfulClusters[0])
			sendCommitOrAbort(successfulClusters, request.Tx, false)
		} else {
			logger.Printf("Transaction %v failed to prepare in both clusters. No action required.", request.Tx.SeqId)
		}
		delete(c.preparedTxns, requestID)
	}

	*reply = true
	return nil
}

func prepareTransaction(clusterID int, tx Transaction) {
	serverIndex := clusterToServer[clusterID] % 3
	leaderPort := ClusterAddresses[clusterID][serverIndex]

	var response bool
	logger.Printf("Sending prepare request to leader at %s for transaction %v", leaderPort, tx)
	ok, err := call(leaderPort, "Paxos.ProcessTransactionByServer", tx, &response)
	if !ok || err != nil {
		logger.Printf("Prepare phase failed for transaction %v in cluster %d: %v", tx, clusterID, err)
	}
}

func processTransaction(tx Transaction) {

	shardX := ShardMapping(tx.Sender)
	shardY := ShardMapping(tx.Receiver)

	key := fmt.Sprintf("%d#%d#%d", tx.SeqId, tx.Sender, tx.Receiver)
	mapLock.Lock()
	startTimes[key] = time.Now()
	mapLock.Unlock()

	if shardX == shardY {
		logger.Println("dataitems are in same cluster. Initiating Intra-shard...")
		clusterID := getClusterID(shardX)
		serverIndex := clusterToServer[clusterID] % 3
		leaderPort := ClusterAddresses[clusterID][serverIndex]
		var response bool
		logger.Println("Calling ProcessTransactionByServer... to leaderport: ", leaderPort)
		call(leaderPort, "Paxos.ProcessTransactionByServer", tx, &response)
		if clusterID == -1 {
			logger.Printf("No server available in Cluster %d for TxID %d", clusterID, tx.SeqId)
			return
		}
	} else {
		logger.Println("dataitems are in different cluster. Initiating Cross-shard...")
		clusterIDX := getClusterID(shardX)
		clusterIDY := getClusterID(shardY)

		if clusterIDX == -1 || clusterIDY == -1 {
			logger.Printf("No servers available for cross-shard TxID %d", tx.SeqId)
			return
		}

		go prepareTransaction(clusterIDX, tx)
		go prepareTransaction(clusterIDY, tx)

	}

}

func processTransactionSet(set TransactionSet) {
	txBySender := make(map[int][]Transaction)

	// log.Printf("cluster : %v", clusterToServer)
	for _, tx := range set.Transactions {
		senderIndex := int(tx.Sender - 1)
		txBySender[senderIndex] = append(txBySender[senderIndex], tx)
	}
	fmt.Println("Processing...")
	for senderIndex, transactions := range txBySender {
		seq := 0
		go func(index int, txs []Transaction) {
			for _, tx := range txs {
				seq += 1
				tx.SeqId = seq
				processTransaction(tx)
			}
		}(senderIndex, transactions)

	}
}

func calcPerformance() {
	var totalLatency int64
	var totalThroughput float64

	for _, elapsedTime := range elapsedTimeMs {
		if elapsedTime > 0 {
			totalLatency += elapsedTime
			totalThroughput += float64(1) / (float64(elapsedTime) / 1000.0)
		}
	}

	if len(elapsedTimeMs) > 0 {
		averageLatency := totalLatency / int64(len(elapsedTimeMs))
		fmt.Printf("Average Latency: %d ms\n", averageLatency)
		fmt.Printf("Total Throughput: %.2f transactions/sec\n", totalThroughput)
	} else {
		fmt.Println("\nNo transactions processed for performance calculation.")
	}

	startTimes = make(map[string]time.Time)
	elapsedTimeMs = make(map[string]int64)
}

func main() {

	initLogger()
	cleanupDBFiles()

	transactionSets, err := ParseTransactionsFromCSV("Test_Cases_-_Lab3.csv")
	if err != nil {
		log.Fatalf("Error parsing transactions: %v", err)
	}
	coordinatorPort := "localhost:1234"
	MakeCoordinator(coordinatorPort)
	logger.Printf("Coordinator running on port %s", coordinatorPort)
	// peers := []string{"localhost:1111", "localhost:2222", "localhost:3333", "localhost:4444", "localhost:5555", "localhost:6666", "localhost:7777", "localhost:8888", "localhost:9999"}
	numServers := 9
	numClusters := 3
	pxa := make([]*Paxos, numServers)
	liveservers := make(map[int]bool)

	serverIndex := 0
	shardId := 0
	for clusterID := 0; clusterID < numClusters; clusterID++ {
		pxa[serverIndex] = Make(serverIndex, clusterID, shardId, ClusterAddresses[clusterID])
		serverIndex++
		pxa[serverIndex] = Make(serverIndex, clusterID, shardId, ClusterAddresses[clusterID])
		serverIndex++
		pxa[serverIndex] = Make(serverIndex, clusterID, shardId, ClusterAddresses[clusterID])
		serverIndex++
		shardId++
	}

	reader := bufio.NewReader(os.Stdin)
	for _, set := range transactionSets {

		fmt.Printf("Press Enter to process Set %d...\n", set.SetNumber)
		reader.ReadString('\n')
		logger.Printf("Started processing transactions of set %d : ", set.SetNumber)
		logger.Println("-------------------------------------------------------------------------------------------")

		for i := 0; i < numServers; i++ {
			liveservers[i] = false
		}

		for _, server := range set.LiveServers {
			liveservers[server] = true
		}
		for i := 0; i < numServers; i++ {
			if !liveservers[i] {
				logger.Printf("liveserver %d is %t so killing", i+1, liveservers[i])
				pxa[i].Kill()
			} else {
				if pxa[i].dead {
					logger.Printf("liveserver %d is true but server %d is dead, restarting", i+1, i+1)
					pxa[i].Restart(i)
				}
			}
		}

		clusterToServer = mapContactServersToClusters(set.ContactServers)

		processTransactionSet(set)
		time.Sleep(2 * time.Second)
		logger.Println("---------------------------------------------------------------------------------------------")
		fmt.Println("\nAll transactions in the set have been processed.")
		fmt.Println("Press Enter to view results...")
		reader.ReadString('\n')

		for _, px := range pxa {
			px.PrintDB()
			fmt.Println()
		}

		fmt.Printf("Set %d - Performance Metrics:\n", set.SetNumber)
		calcPerformance()

		for {
			fmt.Print("\nEnter client ID to see balance (or press Enter to skip): ")
			clientInput, _ := reader.ReadString('\n')
			clientInput = strings.TrimSpace(clientInput)

			if clientInput == "" {
				break
			}

			clientID, err := strconv.Atoi(clientInput)
			if err != nil {
				fmt.Println("Invalid client ID. Please enter a numeric value.")
				continue
			}

			clusterID := getClusterID(ShardMapping(clientID))
			if clusterID == -1 {
				fmt.Printf("Client ID %d does not belong to any cluster.\n", clientID)
			} else {
				fmt.Printf("Balances for client ID %d across all servers in cluster %d:\n", clientID, clusterID)
				for _, px := range pxa {
					if px.clusterId == clusterID {
						px.PrintBalance(clientID)
					}
				}
			}
		}
	}
}
