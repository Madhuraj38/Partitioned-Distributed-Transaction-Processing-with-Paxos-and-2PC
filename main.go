package main

import (
	"bufio"
	"encoding/csv"
	"fmt"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

type Transaction struct {
	SeqId       int
	Sender      int
	Receiver    int
	Amount      int
	CommitIndex int
}

type TransactionSet struct {
	SetNumber      int
	Transactions   []Transaction
	LiveServers    []int
	ContactServers []int
}

var ClusterAddresses = map[int][]string{
	0: {"localhost:1111", "localhost:2222", "localhost:3333"},
	1: {"localhost:4444", "localhost:5555", "localhost:6666"},
	2: {"localhost:7777", "localhost:8888", "localhost:9999"},
}
var clusterToServer = make(map[int]int)

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

func processTransaction(tx Transaction) {

	shardX := ShardMapping(tx.Sender)
	shardY := ShardMapping(tx.Receiver)

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

		// serverAddrX := getRandomServer()
		// serverAddrY := getRandomServer()

		if clusterIDX == -1 || clusterIDY == -1 {
			logger.Printf("No servers available for cross-shard TxID %d", tx.SeqId)
			return
		}
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

func main() {

	initLogger()
	cleanupDBFiles()

	transactionSets, err := ParseTransactionsFromCSV("transactions.csv")
	if err != nil {
		log.Fatalf("Error parsing transactions: %v", err)
	}

	// client := &Client{}
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

		// for clusterID := 0; clusterID < numClusters; clusterID++ {
		// 	contactServerIndex := clusterToServer[clusterID]
		// 	logger.Printf("Server %d is the contact server for cluster %d", contactServerIndex+1, clusterID)
		// 	pxa[contactServerIndex].StartContactServer()
		// }

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
	}
}
