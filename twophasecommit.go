package main

import (
	"database/sql"
	"fmt"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

type Paxos struct {
	mu              sync.Mutex
	peers           []string
	me              int
	clusterId       int
	shardId         int
	Balance         int
	db              *sql.DB
	dead            bool
	l               net.Listener
	ballot          string
	highestBallot   string
	acceptNum       string
	acceptVal       []Transaction
	queue           []Transaction
	committedIndex  int
	locks           sync.Map
	transactionChan chan Transaction
}

type PrepareArgs struct {
	Ballot         string
	CommittedIndex int
}

type PrepareResponse struct {
	Promise        bool
	AcceptNum      string
	AcceptVal      []Transaction
	HighestBallot  string
	NeedSync       bool
	CommittedIndex int
}

type AcceptArgs struct {
	Ballot    string
	MegaBlock []Transaction
}

type AcceptResponse struct {
	Accepted bool
}

type DecideArgs struct {
	Ballot    string
	MegaBlock []Transaction
}

type DecideResponse struct {
	Success bool
}

type SyncArgs struct {
	MegaBlock []Transaction
}

func (px *Paxos) ProcessTransactionByServer(tx Transaction, reply *bool) error {
	px.transactionChan <- tx
	*reply = false
	return nil
}

func (px *Paxos) transactionProcessor() {
	for tx := range px.transactionChan {
		logger.Printf("Processing transaction %v on server %d", tx, px.me+1)

		isIntraShard, _ := px.checkTransactionType(tx)

		if isIntraShard {
			success := px.Start(tx)
			if success {
				logger.Printf("Intra-shard transaction %v committed successfully by server %d.", tx, px.me+1)
				var reply bool
				go call("localhost:1234", "Coordinator.ReceivePrepareResponse",
					struct {
						Tx        Transaction
						ClusterID int
						Status    bool
					}{
						Tx:        tx,
						ClusterID: px.clusterId,
						Status:    success,
					}, &reply)
			} else {
				logger.Printf("Intra-shard transaction %v failed to commit by server %d.", tx, px.me+1)
				var reply bool
				go call("localhost:1234", "Coordinator.ReceivePrepareResponse",
					struct {
						Tx        Transaction
						ClusterID int
						Status    bool
					}{
						Tx:        tx,
						ClusterID: px.clusterId,
						Status:    success,
					}, &reply)
			}
		} else {
			success := px.Start(tx)
			if success {
				logger.Printf("Cross-shard transaction %v prepared successfully by server %d.", tx, px.me+1)
				var reply bool
				go call("localhost:1234", "Coordinator.ReceivePrepareResponse",
					struct {
						Tx        Transaction
						ClusterID int
						Status    bool
					}{
						Tx:        tx,
						ClusterID: px.clusterId,
						Status:    success,
					}, &reply)
			} else {
				logger.Printf("Cross-shard transaction %v failed to prepare by server %d.", tx, px.me+1)
				var reply bool
				go call("localhost:1234", "Coordinator.ReceivePrepareResponse",
					struct {
						Tx        Transaction
						ClusterID int
						Status    bool
					}{
						Tx:        tx,
						ClusterID: px.clusterId,
						Status:    success,
					}, &reply)
			}
		}
	}
}

func (px *Paxos) Start(tx Transaction) bool {
	px.mu.Lock()
	px.ballot = fmt.Sprintf("%d#%d", time.Now().UnixNano(), px.me+1)
	currentBallot := px.ballot
	px.mu.Unlock()

	promiseCount := 0
	var highestAcceptVal []Transaction
	responseChan := make(chan PrepareResponse, len(px.peers))
	timeout := time.After(150 * time.Millisecond)
	px.mu.Lock()
	args := PrepareArgs{Ballot: currentBallot, CommittedIndex: px.committedIndex}
	majority := len(px.peers)/2 + 1
	px.mu.Unlock()

	for i := 0; i < len(px.peers); i++ {
		go func(peer int) {
			var reply PrepareResponse
			logger.Printf("Prepare call requested with ballot num %s from server %d to server %s", currentBallot, px.me+1, px.peers[peer])

			if px.me%3 == peer {
				px.Prepare(&args, &reply)
			} else {
				ok, err := call(px.peers[peer], "Paxos.Prepare", args, &reply)
				if !ok {
					logger.Printf("Prepare call failed for peer %s: %v", px.peers[peer], err)
				}
			}

			responseChan <- reply
			if px.me%3 != peer && reply.NeedSync {
				px.Synchronize(reply.CommittedIndex, peer)
			}
		}(i)
	}

	for {
		select {
		case reply := <-responseChan:
			px.mu.Lock()
			if reply.Promise {
				promiseCount++
				if reply.AcceptNum != "" && reply.AcceptVal != nil {
					if reply.AcceptNum > px.acceptNum {
						px.acceptNum = reply.AcceptNum
						highestAcceptVal = reply.AcceptVal
					}
				}
			}
			px.mu.Unlock()

		case <-timeout:
			logger.Println("Prepare phase timed out, proceeding with available promises")
			close(responseChan)
			if promiseCount >= majority {
				logger.Printf("Majority promises received by server %d, proceeding as leader.", px.me+1)

				if !px.verifyConditions(tx) {
					logger.Printf("Conditions not met for transaction %v. Aborting...", tx)
					return false
				}

				megablock := px.createMegaBlock(highestAcceptVal, tx)
				if len(megablock) > 0 {
					return px.sendAccept(megablock)
				} else {
					logger.Printf("server %d not able to add transaction, aborting...", px.me+1)
					return false
				}
			} else {
				logger.Println("Failed to receive majority promises, aborting...")
				return false
			}
		}
	}
}

func (px *Paxos) Prepare(args *PrepareArgs, reply *PrepareResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	if args.Ballot > px.highestBallot {
		if args.CommittedIndex == px.committedIndex {
			logger.Printf("Prepare call accepted with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
			px.highestBallot = args.Ballot
			reply.Promise = true
			reply.HighestBallot = px.highestBallot
			if px.acceptNum != "" && px.acceptVal != nil {
				reply.AcceptNum = px.acceptNum
				reply.AcceptVal = px.acceptVal
			} else {
				reply.AcceptNum = ""
				reply.AcceptVal = nil
			}
		} else if args.CommittedIndex > px.committedIndex {
			reply.Promise = true
			reply.NeedSync = true
			px.acceptNum = ""
			px.acceptVal = nil
			reply.CommittedIndex = px.committedIndex
		} else {
			parts := strings.Split(args.Ballot, "#")
			if len(parts) == 2 {
				serverID, _ := strconv.Atoi(parts[1])
				if px.me != serverID-1 {
					px.Synchronize(args.CommittedIndex, serverID-1)
				}
			} else {
				logger.Println("Invalid ballot number")
			}
			reply.Promise = true
		}
	} else {
		logger.Printf("Prepare call rejected with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
		reply.Promise = false
		reply.HighestBallot = px.highestBallot
	}
	return nil
}

func (px *Paxos) createMegaBlock(peerTransactions []Transaction, tx Transaction) []Transaction {
	var megablock []Transaction
	if len(peerTransactions) > 0 {
		megablock = peerTransactions
	} else {
		megablock = append(megablock, tx)
	}
	logger.Printf("Transactions : %v", megablock)
	return megablock
}

func (px *Paxos) sendAccept(megablock []Transaction) bool {
	px.mu.Lock()
	args := AcceptArgs{Ballot: px.ballot, MegaBlock: megablock}
	px.mu.Unlock()
	acceptCount := 0
	acceptChan := make(chan bool, len(px.peers))
	majority := len(px.peers)/2 + 1

	for i := range len(px.peers) {
		go func(peer int) {
			var reply AcceptResponse
			logger.Printf("Accept call requested with ballot num %s from server %d to server %s", px.ballot, px.me+1, px.peers[peer])

			if px.me%3 == peer {
				px.Accept(&args, &reply)
			} else {
				ok, err := call(px.peers[peer], "Paxos.Accept", args, &reply)
				if !ok {
					logger.Printf("Accept call failed for peer %s: %v", px.peers[peer], err)
				}
			}

			acceptChan <- reply.Accepted
		}(i)
	}

	for i := 0; i < len(px.peers); i++ {
		accepted := <-acceptChan
		if accepted {
			acceptCount++
		}

		if acceptCount >= majority {
			logger.Printf("Majority accepts received: %d", acceptCount)
			px.sendDecide(megablock)
			return true
		}
	}

	logger.Printf("Failed to receive majority accepts, total accepts: %d", acceptCount)
	close(acceptChan)
	return false

}

func (px *Paxos) Accept(args *AcceptArgs, reply *AcceptResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	for _, tx := range args.MegaBlock {
		isIntraShard, isSender := px.checkTransactionType(tx)
		if isIntraShard {
			if !px.acquireLock(int32(tx.Sender)) || !px.acquireLock(int32(tx.Receiver)) {
				for _, t := range args.MegaBlock {
					px.releaseLock(int32(t.Sender))
					px.releaseLock(int32(t.Receiver))
				}
				reply.Accepted = false
				logger.Printf("Server %d failed to acquire locks for transaction %v during accept phase.", px.me, args.MegaBlock)
				return nil
			}
		} else {
			if isSender {
				if !px.acquireLock(int32(tx.Sender)) {
					px.releaseLock(int32(tx.Sender))
					reply.Accepted = false
					logger.Printf("Server %d failed to acquire lock for Sender %d during cross-shard accept phase.", px.me, tx.Sender)
					return nil
				}
			} else {
				if !px.acquireLock(int32(tx.Receiver)) {
					px.releaseLock(int32(tx.Receiver))
					reply.Accepted = false
					logger.Printf("Server %d failed to acquire lock for Receiver %d during cross-shard accept phase.", px.me, tx.Receiver)
					return nil
				}
			}
		}
	}

	if args.Ballot >= px.highestBallot {
		px.highestBallot = args.Ballot
		px.acceptVal = args.MegaBlock
		px.acceptNum = args.Ballot
		reply.Accepted = true
		logger.Printf("Accept call accepted with ballot num %s by server %d and prev highestballot : %s", args.Ballot, px.me+1, px.highestBallot)
	} else {
		for _, tx := range args.MegaBlock {
			isIntraShard, isSender := px.checkTransactionType(tx)
			if isIntraShard {
				px.releaseLock(int32(tx.Sender))
				px.releaseLock(int32(tx.Receiver))
			} else {
				if isSender {
					px.releaseLock(int32(tx.Sender))
				} else {
					px.releaseLock(int32(tx.Receiver))
				}
			}
		}
		reply.Accepted = false
	}
	return nil
}

func (px *Paxos) sendDecide(megablock []Transaction) {
	px.mu.Lock()
	args := DecideArgs{Ballot: px.ballot, MegaBlock: megablock}
	px.mu.Unlock()
	var leaderReply DecideResponse
	logger.Printf("Decide call requested with ballot num %s from server %d to server %d", px.ballot, px.me+1, px.me+1)
	px.ApplyDecision(&args, &leaderReply)
	if leaderReply.Success {
		logger.Printf("Committed successfully with ballot num %s by server %d", px.ballot, px.me+1)

		var wg sync.WaitGroup
		for i := range len(px.peers) {
			wg.Add(1)
			go func(peer int) {
				defer wg.Done()
				var reply DecideResponse
				if peer != px.me%3 {
					logger.Printf("Decide call requested with ballot num %s from server %d to server %s", px.ballot, px.me+1, px.peers[peer])
					ok, err := call(px.peers[peer], "Paxos.ApplyDecision", args, &reply)

					if !ok {
						logger.Printf("Decide request failed for peer %d: %v", peer, err)
					}
				} else {
					return
				}

				if reply.Success {
					logger.Printf("Committed successfully with ballot num %s by server %s", px.ballot, px.peers[peer])
				} else {
					logger.Printf("Decide call rejected with ballot num %s by server %s", px.ballot, px.peers[peer])
				}
			}(i)
		}
		wg.Wait()
	} else {
		logger.Printf("Decide call rejected with ballot num %s by server %d", px.ballot, px.me+1)
	}
}

func (px *Paxos) ApplyDecision(args *DecideArgs, reply *DecideResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	// var maxID int
	// err := px.db.QueryRow("SELECT IFNULL(max(id), 0) FROM committed_log").Scan(&maxID)
	// if err != nil {
	// 	return fmt.Errorf("failed to fetch max ID: %w", err)
	// }

	// newID := maxID + 1
	// if newID == px.committedIndex+1 {
	for _, tx := range args.MegaBlock {
		isIntraShard, isSender := px.checkTransactionType(tx)
		if isIntraShard {
			var senderBalance int
			err := px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Sender).Scan(&senderBalance)
			if err != nil {
				logger.Printf("failed to fetch balance: %v", err)
				reply.Success = false
				return err
			}
			var receiverBalance int
			err = px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Receiver).Scan(&receiverBalance)
			if err != nil {
				logger.Printf("failed to fetch balance: %v", err)
				reply.Success = false
				return err
			}
			_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", senderBalance-tx.Amount, tx.Sender)
			_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", receiverBalance+tx.Amount, tx.Receiver)

			_, err = px.db.Exec("INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
				tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, "C")
			if err != nil {
				logger.Printf("Error committing transaction on server %d: %v", px.me+1, err)
				reply.Success = false
				return err
			}
			px.releaseLock(int32(tx.Sender))
			px.releaseLock(int32(tx.Receiver))
			logger.Printf("Locks released for transaction: Sender %d, Receiver %d", tx.Sender, tx.Receiver)
		} else {
			if isSender {
				var senderBalance int
				err := px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Sender).Scan(&senderBalance)
				if err != nil {
					logger.Printf("failed to fetch balance: %v", err)
					reply.Success = false
					return err
				}
				_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", senderBalance-tx.Amount, tx.Sender)

				px.insertWAL(tx.SeqId, tx.Sender, senderBalance, senderBalance-tx.Amount)
			} else {
				var receiverBalance int
				err := px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Receiver).Scan(&receiverBalance)
				if err != nil {
					logger.Printf("failed to fetch balance: %v", err)
					reply.Success = false
					return err
				}
				_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", receiverBalance+tx.Amount, tx.Receiver)

				px.insertWAL(tx.SeqId, tx.Receiver, receiverBalance, receiverBalance+tx.Amount)
			}

			_, err := px.db.Exec("INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
				tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, "P")
			if err != nil {
				logger.Printf("Error committing transaction on server %d: %v", px.me+1, err)
				reply.Success = false
				return err
			}
		}
	}
	var newID int
	px.db.QueryRow("SELECT IFNULL(max(id), 0) FROM committed_log").Scan(&newID)
	px.committedIndex = newID
	px.acceptNum = ""
	px.acceptVal = nil
	reply.Success = true
	// } else {
	// 	reply.Success = false
	// }
	return nil
}

func (px *Paxos) SynchronizeCommit(args *SyncArgs, reply *DecideResponse) error {
	px.mu.Lock()
	defer px.mu.Unlock()
	for _, tx := range args.MegaBlock {
		var senderBalance int
		err := px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Sender).Scan(&senderBalance)
		if err != nil {
			logger.Printf("failed to fetch balance: %v", err)
			reply.Success = false
			return err
		}
		var receiverBalance int
		err = px.db.QueryRow("SELECT balance FROM Balances where clientID = ?", tx.Receiver).Scan(&receiverBalance)
		if err != nil {
			logger.Printf("failed to fetch balance: %v", err)
			reply.Success = false
			return err
		}
		_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", senderBalance-tx.Amount, tx.Sender)
		_, _ = px.db.Exec("UPDATE Balances set balance = ? Where clientId = ?", receiverBalance+tx.Amount, tx.Receiver)

		_, err = px.db.Exec("INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
			tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, tx.Status)
		if err != nil {
			logger.Printf("Error committing sync transaction on server %d: %v", px.me+1, err)
			reply.Success = false
			return err
		} else {
			var newID int
			px.db.QueryRow("SELECT IFNULL(max(id), 0) FROM committed_log").Scan(&newID)
			// px.committedIndex = tx.CommitIndex
			px.committedIndex = newID
		}
	}
	reply.Success = true
	return nil
}
func (px *Paxos) Synchronize(commitIndex int, peer int) bool {
	count := px.committedIndex - commitIndex
	for idx := commitIndex + 1; idx <= px.committedIndex; idx++ {
		rows, err := px.db.Query("SELECT seqId, sender, receiver, amount, status FROM committed_log WHERE id = ?", idx)
		if err != nil {
			logger.Printf("Error fetching committed transactions: %v", err)
			return false
		}
		defer rows.Close()

		var missedTransactions []Transaction
		for rows.Next() {
			var tx Transaction
			err := rows.Scan(&tx.SeqId, &tx.Sender, &tx.Receiver, &tx.Amount, &tx.Status)
			if err != nil {
				logger.Printf("Error scanning committed transaction: %v", err)
				return false
			}
			missedTransactions = append(missedTransactions, tx)
		}
		args := SyncArgs{MegaBlock: missedTransactions}
		var reply DecideResponse
		logger.Printf("Sync requested from server %d to server %s", px.me+1, px.peers[peer%3])
		ok, err := call(px.peers[peer%3], "Paxos.SynchronizeCommit", &args, &reply)

		if !ok {
			logger.Printf("Sync failed for peer %d: %v", peer, err)
			return false
		}
		if reply.Success {
			count = count - 1
		}
	}
	if count == 0 {
		logger.Printf("Successfully Sync from server %d to server %s", px.me+1, px.peers[peer%3])
		return true
	}
	logger.Printf("Sync failed from server %d to server %s", px.me+1, px.peers[peer%3])
	return false
}

func (px *Paxos) checkTransactionType(tx Transaction) (bool, bool) {
	shardX := ShardMapping(tx.Sender)
	shardY := ShardMapping(tx.Receiver)
	isIntraShard := shardX == shardY

	senderClusterID := getClusterID(shardX)
	isSender := px.clusterId == senderClusterID

	logger.Printf("Server %d: Transaction %v - isIntraShard: %v, senderExists: %v",
		px.me+1, tx, isIntraShard, isSender)

	return isIntraShard, isSender
}

func (px *Paxos) insertWAL(txnId int, clientId int, oldBalance int, newBalance int) {
	_, err := px.db.Exec("INSERT INTO WAL (txnId, clientID, oldbalance, newbalance) VALUES (?, ?, ?, ?)", txnId, clientId, oldBalance, newBalance)
	if err != nil {
		logger.Printf("Failed to insert WAL entry for txnId %d: %v", txnId, err)
	} else {
		logger.Printf("WAL entry inserted for txnId %d: clientID %d, oldbalance %d, newbalance %d", txnId, clientId, oldBalance, newBalance)
	}
}

func (px *Paxos) deleteWAL(txnId int, clientId int) {
	_, err := px.db.Exec("DELETE FROM WAL WHERE txnId = ? AND clientID = ?", txnId, clientId)
	if err != nil {
		logger.Printf("Failed to delete WAL entry for txnId %d and sender %d: %v", txnId, clientId, err)
	} else {
		logger.Printf("WAL entry deleted for txnId %d and clientId %d", txnId, clientId)
	}
}

func (px *Paxos) CommitTransaction(tx Transaction, reply *bool) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, err := px.db.Exec(
		"INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
		tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, "C",
	)
	if err != nil {
		logger.Printf("Failed to commit transaction %v on server %d: %v", tx, px.me+1, err)
		*reply = false
		return err
	}
	_, isSender := px.checkTransactionType(tx)

	if isSender {
		px.deleteWAL(tx.SeqId, tx.Sender)
		px.releaseLock(int32(tx.Sender))
	} else {
		px.deleteWAL(tx.SeqId, tx.Receiver)
		px.releaseLock(int32(tx.Receiver))
	}

	logger.Printf("Transaction %v committed successfully on server %d.", tx, px.me+1)
	*reply = true
	return nil
}

func (px *Paxos) AbortTransaction(tx Transaction, reply *bool) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	_, isSender := px.checkTransactionType(tx)

	if isSender {
		if px.restoreWAL(tx.SeqId, tx.Sender) {
			_, err := px.db.Exec(
				"INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
				tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, "A",
			)
			if err != nil {
				logger.Printf("Failed to abort transaction %v on server %d: %v", tx, px.me+1, err)
				*reply = false
				return err
			}
		}
		px.deleteWAL(tx.SeqId, tx.Sender)
		px.releaseLock(int32(tx.Sender))
	} else {
		if px.restoreWAL(tx.SeqId, tx.Receiver) {
			_, err := px.db.Exec(
				"INSERT INTO committed_log (seqId, sender, receiver, amount, status) VALUES (?, ?, ?, ?, ?)",
				tx.SeqId, tx.Sender, tx.Receiver, tx.Amount, "A",
			)
			if err != nil {
				logger.Printf("Failed to abort transaction %v on server %d: %v", tx, px.me+1, err)
				*reply = false
				return err
			}
		}
		px.deleteWAL(tx.SeqId, tx.Receiver)
		px.releaseLock(int32(tx.Receiver))
	}

	logger.Printf("Transaction %v aborted successfully on server %d.", tx, px.me+1)
	*reply = true
	return nil
}

func (px *Paxos) restoreWAL(txnId int, clientId int) bool {
	row := px.db.QueryRow(
		"SELECT oldbalance FROM WAL WHERE txnId = ? AND clientID = ?",
		txnId, clientId,
	)
	var oldBalance int
	err := row.Scan(&oldBalance)

	if err == nil {
		_, err = px.db.Exec(
			"UPDATE Balances SET balance = ? WHERE clientID = ?",
			oldBalance, clientId,
		)
		if err != nil {
			logger.Printf("Failed to restore balance for sender %d from WAL: %v", clientId, err)
			return false
		}
		logger.Printf("Sender %d balance restored to %d from WAL on server %d", clientId, oldBalance, px.me+1)
		return true
	}

	logger.Printf("No WAL entry found for transaction %d client %d on server %d", txnId, clientId, px.me+1)
	return false
}

func (px *Paxos) verifyConditions(tx Transaction) bool {
	px.mu.Lock()
	defer px.mu.Unlock()

	isIntraShard, isSender := px.checkTransactionType(tx)

	if isIntraShard {
		if px.isLocked(int32(tx.Sender)) || px.isLocked(int32(tx.Receiver)) {
			logger.Printf("Transaction %v cannot proceed. Locks held on data items.", tx)
			return false
		}

		var senderBalance int
		err := px.db.QueryRow("SELECT balance FROM Balances WHERE clientID = ?", tx.Sender).Scan(&senderBalance)
		if err != nil {
			logger.Printf("Failed to fetch balance for client %d: %v", tx.Sender, err)
			return false
		}

		if senderBalance < tx.Amount {
			logger.Printf("Insufficient balance for transaction %v. Sender balance: %d", tx, senderBalance)
			return false
		}
	} else {
		if isSender {
			if px.isLocked(int32(tx.Sender)) {
				logger.Printf("Transaction %v cannot proceed. Locks held on data items.", tx)
				return false
			}

			var senderBalance int
			err := px.db.QueryRow("SELECT balance FROM Balances WHERE clientID = ?", tx.Sender).Scan(&senderBalance)
			if err != nil {
				logger.Printf("Failed to fetch balance for client %d: %v", tx.Sender, err)
				return false
			}

			if senderBalance < tx.Amount {
				logger.Printf("Insufficient balance for transaction %v. Sender balance: %d", tx, senderBalance)
				return false
			}
		} else {
			if px.isLocked(int32(tx.Receiver)) {
				logger.Printf("Transaction %v cannot proceed. Locks held on data items.", tx)
				return false
			}
		}
	}

	logger.Printf("Conditions met for transaction %v. Proceeding to accept phase.", tx)
	return true
}

func (px *Paxos) initLock(id int32) *int32 {
	val, ok := px.locks.Load(id)
	if !ok {
		newLock := int32(0)
		px.locks.Store(id, &newLock)
		return &newLock
	}
	return val.(*int32)
}

func (px *Paxos) acquireLock(id int32) bool {
	lock := px.initLock(id)
	acquired := atomic.CompareAndSwapInt32(lock, 0, 1)

	if acquired {
		logger.Printf("Lock acquired for resource %d by server %d", id, px.me)
	} else {
		logger.Printf("Failed to acquire lock for resource %d by server %d", id, px.me)
	}

	return acquired
}

func (px *Paxos) releaseLock(id int32) {
	lock := px.initLock(id)

	if atomic.LoadInt32(lock) == 0 {
		logger.Printf("Lock for resource %d is already released by server %d", id, px.me)
		return
	}
	atomic.StoreInt32(lock, 0)
	logger.Printf("Lock released for resource %d by server %d", id, px.me)
}

func (px *Paxos) isLocked(id int32) bool {
	lock := px.initLock(id)
	return atomic.LoadInt32(lock) == 1
}

func Make(me, clusterID, shardID int, peers []string) *Paxos {
	db, err := sql.Open("sqlite3", fmt.Sprintf("server_%d.db", me))
	if err != nil {
		logger.Fatalf("Failed to open database: %v", err)
	}

	px := &Paxos{}
	px.mu = sync.Mutex{}
	px.peers = peers
	px.me = me
	px.clusterId = clusterID
	px.shardId = shardID
	px.db = db
	px.Balance = 10
	px.ballot = ""
	px.acceptVal = nil
	px.acceptNum = ""
	px.queue = nil
	px.committedIndex = 0
	px.transactionChan = make(chan Transaction, 1000)

	go px.transactionProcessor()

	_, err = px.db.Exec(`CREATE TABLE IF NOT EXISTS Balances (
        clientID INTEGER PRIMARY KEY,
        balance INTEGER
    );`)
	if err != nil {
		logger.Fatalf("Failed to create Balances table: %v", err)
	}

	_, err = px.db.Exec(`CREATE TABLE IF NOT EXISTS committed_log (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
		seqId INTEGER,
        sender INTEGER,
        receiver INTEGER,
        amount INTEGER,
		status TEXT
    );`)
	if err != nil {
		logger.Fatalf("Failed to create Logs table: %v", err)
	}

	_, err = px.db.Exec(`CREATE TABLE IF NOT EXISTS WAL (
		txnId INTEGER ,
		clientID INTEGER,
		oldbalance INTEGER,
		newbalance INTEGER
	);`)
	if err != nil {
		logger.Fatalf("Failed to create WAL table: %v", err)
	}

	px.initializeBalances(shardID)

	rpcs := rpc.NewServer()
	rpcs.Register(px)

	l, e := net.Listen("tcp", peers[me%3])
	if e != nil {
		logger.Fatal("listen error:", e)
	}
	px.l = l

	go func() {
		for {
			if !px.dead {
				conn, err := px.l.Accept()
				if err == nil && !px.dead {
					go rpcs.ServeConn(conn)
				} else if err != nil && !px.dead {
					// logger.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}
	}()

	return px
}

func (px *Paxos) initializeBalances(id int) {
	tx, err := px.db.Begin()
	if err != nil {
		logger.Fatalf("Failed to begin transaction: %v", err)
	}

	stmt, err := tx.Prepare("INSERT OR IGNORE INTO Balances (clientID, balance) VALUES (?, ?);")
	if err != nil {
		logger.Fatalf("Failed to prepare statement: %v", err)
	}
	defer stmt.Close()

	for i := (id * 1000) + 1; i <= (id+1)*1000; i++ {
		_, err = stmt.Exec(i, 10)
		if err != nil {
			tx.Rollback()
			logger.Fatalf("Failed to insert initial balance for ClientID %d: %v", i, err)
		}
	}

	err = tx.Commit()
	if err != nil {
		logger.Fatalf("Failed to commit transaction: %v", err)
	}
}

func (px *Paxos) Kill() {
	px.mu.Lock()
	defer px.mu.Unlock()
	px.dead = true

	if px.l != nil {
		px.l.Close()
	}
	logger.Printf("Server %d set to inactive", px.me+1)
}

func (px *Paxos) Restart(me int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.dead {
		px.dead = false
		var err error
		px.l, err = net.Listen("tcp", px.peers[me%3])
		// logger.Println("peer for address : ", px.peers[me%3])
		if err != nil {
			logger.Fatalf("Failed to reopen listener for server %d: %v", px.me+1, err)
		}
	} else {
		logger.Printf("Server %d is already running, no need to restart", px.me+1)
	}
}

func call(srv string, name string, args interface{}, reply interface{}) (bool, error) {
	c, err := rpc.Dial("tcp", srv)
	if err != nil {
		logger.Printf("Failed to connect to peer %s : %v", srv, err)
		return false, err
	}
	defer c.Close()
	err = c.Call(name, args, reply)
	if err == nil {
		return true, err
	}

	logger.Println(err)
	return false, err
}

func cleanupDBFiles() {
	files, err := os.ReadDir(".")
	if err != nil {
		logger.Printf("Error reading directory: %v", err)
		return
	}

	for _, file := range files {
		if !file.IsDir() && strings.HasSuffix(file.Name(), ".db") {
			err := os.Remove(file.Name())
			if err != nil {
				logger.Printf("Error deleting file %s: %v", file.Name(), err)
			} else {
				logger.Printf("Deleted file: %s", file.Name())
			}
		}
	}
}

func (px *Paxos) PrintDB() {
	px.mu.Lock()
	defer px.mu.Unlock()

	fmt.Printf("Server %d committed log:\n", px.me+1)
	rows, err := px.db.Query("SELECT id, seqId, sender, receiver, amount, status FROM committed_log")
	if err != nil {
		logger.Printf("Error querying committed log: %v", err)
		return
	}
	defer rows.Close()

	for rows.Next() {
		var sender, receiver, status string
		var amount, id, seqId int
		err := rows.Scan(&id, &seqId, &sender, &receiver, &amount, &status)
		if err != nil {
			logger.Printf("Error scanning row: %v", err)
			continue
		}
		fmt.Printf(" %d. %d - %s -> %s: %d -- %s\n", id, seqId, sender, receiver, amount, status)
	}
}

func (px *Paxos) PrintBalance(clientID int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	var balance int
	px.db.QueryRow("SELECT balance FROM Balances WHERE clientID = ?", clientID).Scan(&balance)

	fmt.Printf("Balance for client %d on server %d: %d\n", clientID, px.me+1, balance)
}
