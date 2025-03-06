import os
import socket
import threading
import sys
from collections import deque
import json
import time
import random
import queue
import logging
from datetime import datetime
from messages import *
from messages import Message
from shard_manager import ShardManager
from log_entry import LogEntry
from transaction import Transaction

# Custom formatter to truncate function names
class TruncatingFormatter(logging.Formatter):
    def format(self, record):
        # Truncate the function name to 15 characters
        record.funcName = record.funcName[:10].ljust(10)
        return super().format(record)

# Configure logging
logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)

# Create a handler and set the custom formatter
handler = logging.StreamHandler(sys.stdout)
formatter = TruncatingFormatter(
    fmt='[%(funcName)s] %(message)s',
    datefmt='%M:%S'  # Minutes:Seconds
)
handler.setFormatter(formatter)
logger.addHandler(handler)

DECISION_TIMEOUT = 8
HEARTBEAT_INTERVAL = 3          # seconds
ELECTION_TIMEOUT_RANGE = (1, 10) # seconds
TRANSACTION_TIMEOUT = 5         # Timeout for acquiring locks (seconds)
SHARD_SIZE = 1000
RED  = '\033[31m'
RESET = '\033[0m'

class Server:
    def __init__(self, my_ip, my_port, my_cluster, server_config, coordinator_addr):
        self.transaction_queue = queue.Queue()
        self.my_address = (my_ip, my_port)
        self.my_cluster = my_cluster
        self.coordinator_addr = coordinator_addr

        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.my_address)

        self.cluster_to_servers = {}
        self.SERVER_NAMES = {("127.0.0.1", 5000): "client"} # hardcode the client
        for server in server_config:
            addr = server["ip"]
            port = server["port"]
            cluster = server["cluster"]
            server_id = server["id"]

            if cluster == self.my_cluster:
                self.SERVER_NAMES[(addr, port)] = server_id
            
            # never message oneself by removing from data
            if port != my_port:
                if cluster not in self.cluster_to_servers:
                    self.cluster_to_servers[cluster] = []
                self.cluster_to_servers[cluster].append({"id": server_id, "addr": (addr,port)})

        self.running = True  # flag to control running state of listener thread
        self.shard_start = (my_cluster - 1) * 1000 + 1
        self.shard_end = my_cluster * 1000
        self.shardManager = ShardManager(self.SERVER_NAMES[(self.my_address)], self.my_cluster)

        # RAFT variables
        self.current_term = 0
        self.current_leader = None
        self.voted_for = None

        self.log = []  # Transaction log
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = -1
        if os.path.exists(f"shards/{server_id}_log.json"):
            self.log, self.commit_index = self.shardManager.get_log()
            self.last_applied = self.commit_index
        # else
        #   create new log file as normal
        #   write over balance table
        #   ^ these are both done in ShardManager.init()

        self.next_index = {} # Index of the next log entry to send to each follower
        self.match_index = {} # Index of the highest log entry known to be replicated on a server
        self.role = "follower"  # Initial state is follower
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)  # Randomized timeout for leader election
        self.last_heartbeat = time.time()  # Track leader's last heartbeat
        self.replication_mode = False  # Flag to indicate active replication
        self.append_ack_queue = queue.Queue()  # Queue for replication mode APPEND_ACKs


        # Initialize data store for transactions
        self.data_store = {id: 10 for id in range(self.shard_start, self.shard_end + 1)}
        # Tracks which accounts are locked
        self.locks = {id: False for id in range(self.shard_start, self.shard_end + 1)}
        
        self.pending_decisions = {}  # Track pending 2PC transactions {tx_id: (timestamp, transaction)}
        self.pending_decisions_lock = threading.Lock()  # To prevent race conditions
        threading.Thread(target=self.check_pending_decisions, daemon=True).start()

    def check_pending_decisions(self):
        while True:
            current_time = time.time()
            with self.pending_decisions_lock:
                if self.pending_decisions:
                    for tx_id, (start_time, transaction) in list(self.pending_decisions.items()):
                        if current_time - start_time > DECISION_TIMEOUT:
                            logger.info(f"Transaction {tx_id} timed out. Releasing locks and sending Ack.")
                            self.release_locks_for_transaction(tx_id)
                            ack_message = Ack(tx_id=tx_id).to_dict()
                            self.send_message(ack_message, self.coordinator_addr)
                time.sleep(0.3)

    def release_locks_for_transaction(self, tx_id):
        with self.pending_decisions_lock:
            _, transaction = self.pending_decisions.get(tx_id, (None, None))
            if transaction:
                sender = transaction[0]
                receiver = transaction[1]
                if self.shardManager.is_account_in_cluster(sender):
                    self.locks[sender] = False
                    logger.info(f"Released lock on sender {sender} for transaction {tx_id}.")
                if self.shardManager.is_account_in_cluster(receiver):
                    self.locks[receiver] = False
                    logger.info(f"Released lock on receiver {receiver} for transaction {tx_id}.")
            if tx_id in self.pending_decisions:
                del self.pending_decisions[tx_id]
    
    def step_down_to_follower(self, new_term):
        """Step down to follower upon receiving a higher term."""
        self.role = "follower"
        self.current_term = new_term
        self.voted_for = None

    def become_leader(self):
        """Transition to leader and start sending heartbeats."""
        self.role = "leader"
        logger.info(f"{self.SERVER_NAMES[self.my_address]} is now the leader for term {self.current_term}")
        # update next_index for all followers
        for server_addr in self.SERVER_NAMES:
            self.next_index[server_addr] = len(self.log)
        # Start heartbeat thread
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def start_election(self):
        # Start a new election
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.my_address
        votes_received = 1  # Vote for self


        message = RequestVote(
            term=self.current_term, 
            candidate_id=self.my_address, 
            last_log_index=len(self.log)-1, 
            last_log_term=self.log[-1].term if self.log else None
            ).to_dict()
        # Request votes from other servers
        self.clustercast(message, self.my_cluster)

        # Wait for votes
        start_time = time.time()
        while time.time() - start_time < self.election_timeout:
            try:
                data, addr = self.socket.recvfrom(4096)
                response = json.loads(data.decode('utf-8'))
                logger.info(f"Received message type {response.get('msg_type')} from {addr}")

                # Step down if a higher term message is received
                if response.get("term", 0) > self.current_term:
                    # logger.info(f"Received higher term {response.get('term')}, stepping down to follower.")
                    self.step_down_to_follower(response.get("term"))
                    return  # Stop election process
                
                # AppendEntries RPC received from new leader: step down
                if response.get("msg_type") == "APPEND_ENTRIES" and response.get("term", 0) >= self.current_term:
                    logger.info(f"Received AppendEntries RPC from {response.get('leader_id')}, stepping down to follower.")
                    self.step_down_to_follower(response.get("term"))
                    self.last_heartbeat = time.time()  # Reset election timeout
                    return  # Stop election process
                
                # Count votes
                if response.get("msg_type") == "VOTE_RESPONSE" and response.get("term") == self.current_term:
                    votes_received += 1
                
                # Become leader if majority votes received
                if votes_received > 1: # Majority
                    self.become_leader()
                    return  # Exit election loop
                
            except socket.timeout:
                break
        
        # if no outcome, restart election
        if self.role == "candidate":
            time.sleep(random.uniform(2, 5)) # Prevent election collisions with random election delay
            self.start_election()


    def send_heartbeats(self):
        """Send periodic heartbeats (empty AppendEntries RPC) to maintain authority."""
        while self.role == "leader":
            message = AppendEntries(
                term=self.current_term, 
                leader_id=self.my_address, 
                prev_log_index=len(self.log)-1, 
                prev_log_term=self.log[-1].term if self.log else None, 
                entries=[], 
                leader_commit=self.commit_index
                ).to_dict()
            
            logger.info(f"HEARTBEAT")
            self.clustercast(message, self.my_cluster)
            time.sleep(HEARTBEAT_INTERVAL)
            
            # If a higher term is received, leader should step down
            if self.role != "leader":
                logger.info("Stepping down from leader role, stopping heartbeats.")
                break

    def handle_append_entries(self, message, addr):
        """Handle incoming AppendEntries RPC: heartbeats & log replication."""
        term = message.get("term")
        leader_id = message.get("leader_id")
        prev_log_index = message.get("prev_log_index")
        prev_log_term = message.get("prev_log_term")
        entries = message.get("entries", [])
        leader_commit = message.get("leader_commit", -1)

        if len(entries) == 0:
            logger.info(f"HEARTBEAT from {leader_id} for term {term}")

        if prev_log_index is None:
            prev_log_index = -1
        if prev_log_term is None:
            prev_log_term = 0

        # Reject if term < current term
        if term < self.current_term:
            logger.info(f"Received outdated RPC from leader {leader_id} for term (term {term} < {self.current_term}), ignoring.")
            return

        # Step down if term is higher
        if term > self.current_term:
            self.role = "follower"
            self.current_term = term
            self.voted_for = None # Reset vote
        
        self.last_heartbeat = time.time() # Reset election timeout

        # Update known leader on heartbeat
        if self.role == "follower":
            self.current_leader = tuple(leader_id)
            # logger.info(f"Updated current leader to {leader_id}")

        # Reject if log doesn’t contain an entry at prev_log_index or term doesn't match
        # FIXME: Check if prev_log_index is -1????    
        if (prev_log_index != -1) and (prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term):
            logger.info(f"Log mismatch at index {prev_log_index}, rejecting AppendEntries from {leader_id}")
            # FIXME: unlock accounts???
            response = AppendAck(success=False).to_dict()
            self.send_message(response, addr)
            return
        
        # If existing entries conflict with new entries, delete all existing entries starting with first conflicting entry
        if prev_log_index + 1 < len(self.log): # there are conflicting entries
            self.log = self.log[:prev_log_index + 1] # delete conflicting entries
            logger.info(f"DELETING LOG ENTIRES: 0-{prev_log_index+1}!")
            self.shardManager.truncate_log(prev_log_index+1)

        # Append any new entries not in log
        for entry in entries:
            is_2PC = entry["is_2PC"]
            transaction=entry["transaction"]
            if transaction is not None:
                sender = transaction["sender"]
                receiver = transaction["receiver"]
                logger.info(f"sender: {sender}, receiver: {receiver}")
                logger.info(f"entry: {entry}")
                
            tx_id = entry.get("tx_id")  # Extract tx_id safely
            committed_2PC = entry.get("committed_2PC", False)
            # Create transaction object from dict
            if not is_2PC and isinstance(transaction, dict):
                transaction = Transaction(transaction["sender"], transaction["receiver"], transaction["amount"])

            # Lock accounts on followers
            # self.locks.setdefault(sender, False)
            # self.locks.setdefault(receiver, False)
            if is_2PC and transaction is not None:
                if self.shardManager.is_account_in_cluster(sender):
                    self.locks[sender] = True
                    with self.pending_decisions_lock:
                        logger.info(f"tx_id: {tx_id}, sender: {sender}, receiver: {receiver}")
                        self.pending_decisions[tx_id] = (time.time(), [sender, receiver])
                        logger.info(f"pending_decisions: {self.pending_decisions}")
                elif self.shardManager.is_account_in_cluster(receiver):
                    self.locks[receiver] = True
                    with self.pending_decisions_lock:
                        logger.info(f"tx_id: {tx_id}, sender: {sender}, receiver: {receiver}")
                        self.pending_decisions[tx_id] = (time.time(), [sender, receiver])
                        logger.info(f"pending_decisions: {self.pending_decisions}")
            elif not is_2PC:
                self.locks[sender] = True
                self.locks[receiver] = True

            e = LogEntry(term=entry["term"], transaction=transaction, is_2PC=is_2PC, tx_id=tx_id, committed_2PC=committed_2PC)
            self.shardManager.append_to_log(e)
            self.log.append(e)
            logger.info(f"Appended new log entry from leader {leader_id}: term: {term}, {entry['transaction']}")

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

            # Advance state machine with newly committed entries
            if self.commit_index > -1:
                self.apply_committed_entries()

        # Send ACK to leader
        response = AppendAck(success=True).to_dict()
        self.send_message(response, addr)

        logger.info(f"commit index: {self.commit_index}")
                
    def handle_vote_request(self, message, addr):
        """Handle incoming RequestVote RPC."""
        term = message.get("term")
        candidate_id = tuple(message.get("candidate_id"))
        last_log_index = message.get("last_log_index")
        last_log_term = message.get("last_log_term")
        
        if last_log_index is None:
            last_log_index = -1
        if last_log_term is None:
            last_log_term = 0

        # Step down if term is higher
        if term > self.current_term:
            self.current_term = term
            self.role = "follower"
            self.voted_for = None  # Reset vote
        
        # Reject vote if already voted in this term
        if self.voted_for is not None:
            logger.info(f"Already voted for {self.voted_for} in term {self.current_term}, rejecting {candidate_id}")
            return
        
        # Reject if candidate's log is less complete
        my_last_log_index = len(self.log) - 1
        my_last_log_term = self.log[-1].term if self.log else 0
        if (last_log_term < my_last_log_term) or (last_log_term == my_last_log_term and last_log_index < my_last_log_index):
            logger.info(f"Rejecting vote request from {candidate_id} (outdated log).")
            return

        # Grant vote
        self.voted_for = candidate_id
        response = VoteResponse(term=self.current_term, vote_granted=True).to_dict()
        self.send_message(response, addr)
        logger.info(f"Voted for {candidate_id} in term {term}")

    def enqueue_transaction(self, message, addr):
        """Adds a transaction request to the queue."""
        # Add transaction to queue
        self.transaction_queue.put((message, addr, False))  # False for is_2PC transaction
        logger.info(f"[{self.my_address}] Successfully added transaction to queue: {message}")

    def enqueue_cross_shard_transaction(self, message, addr):
        """Enqueue cross-shard transactions for 2PC handling."""
        # Add transaction to queue
        self.transaction_queue.put((message, addr, True))  # True for is_2PC transaction
        logger.info(f"[{self.my_address}] Enqueued cross-shard transaction: {message}")
    
    def process_transaction_queue(self):
        """Continuously processes transactions from the queue."""
        while True:
            try:
                message, addr, is_2PC = self.transaction_queue.get()  # get next transaction and dequeue
                logger.info(f"[{self.my_address}] Processing transaction from queue: {message}")
                self.process_transaction(message, addr, is_2PC)  # Process transaction normally
            finally:
                self.transaction_queue.task_done()
    
    def handle_client_request(self, message, addr):
        """Handles client requests by redirecting to the leader if necessary or adding transactions to the queue."""
        if self.role == "leader":
            # If this server is the leader, process the request
            self.enqueue_transaction(message, addr)
        else:
            # Forward request to leader
            if self.current_leader:
                logger.info(f"Redirecting client request to leader at {self.current_leader}")
                self.send_message(message, self.current_leader)
            else:
                logger.info("Error: No known leader to forward request.")

    def handle_prepare(self, message, addr):
        if self.role == "leader":
            # If this server is the leader, process the request
            self.enqueue_cross_shard_transaction(message, addr)
        else:
            # Forward request to leader
            if self.current_leader:
                logger.info(f"Redirecting client request to leader at {self.current_leader}")
                self.send_message(message, self.current_leader)
            else:
                logger.info("Error: No known leader to forward request.")

    def process_transaction(self, message, addr, is_2PC):
        """Processes a single transaction request."""
        """Handles intra-shard client request by adding a new log entry and replicating it to followers."""
        sender = int(message.get("sender"))
        receiver = int(message.get("receiver"))
        amount = int(message.get("amount"))
        tx_id = message.get("tx_id")
        logger.info(f"Received client request: {sender} sends ${amount} to {receiver}, cross-shard: {is_2PC}")
        
        # Check if sender has sufficient balance
        if self.shardManager.is_account_in_cluster(sender) and self.shardManager.get_balance(sender) < amount:
            logger.info(f"Transaction rejected: {sender} has insufficient balance.")
            if is_2PC:
                # For 2PC: vote No
                vote = Vote(tx_id=tx_id, vote = "no").to_dict()
                self.send_message(vote, self.coordinator_addr)
            else:
                self.send_message(ClientResponse(False, sender, receiver, amount).to_dict(), self.coordinator_addr)
            return
        
        if is_2PC:
            if self.shardManager.is_account_in_cluster(sender):
                # sender shard
                if self.locks[sender]:
                    # account locked: abort
                    logger.info(f"Transaction rejected: {sender} account locked.")
                    vote = Vote(tx_id=tx_id, vote = "no").to_dict()
                    self.send_message(vote, self.coordinator_addr)
                    return
            elif self.shardManager.is_account_in_cluster(receiver):
                # receiver shard
                if self.locks[receiver]:
                    # account locked: abort
                    logger.info(f"Transaction rejected: {receiver} account locked.")
                    vote = Vote(tx_id=tx_id, vote = "no").to_dict()
                    self.send_message(vote, self.coordinator_addr)
                    return
        if not is_2PC:
            # Wait until both accounts are unlocked
            start_time = time.time()
            while self.locks[sender] or self.locks[receiver]:
                logger.info(f"Waiting for {sender} and {receiver} to unlock...")
                if time.time() - start_time > TRANSACTION_TIMEOUT:
                    logger.info(f"Transaction rejected: Timeout while waiting for {sender} or {receiver} to unlock.")
                    return  # Abort the transaction
                time.sleep(0.1)  # Short delay
        
        # Conditions met: lock both sender and receiver
        if not is_2PC:
            self.locks[sender] = True
            self.locks[receiver] = True
        else:
            if self.shardManager.is_account_in_cluster(sender):
                # lock sender
                self.locks[sender] = True
            elif self.shardManager.is_account_in_cluster(receiver):
                # lock receiver
                self.locks[receiver] = True

        # Create log entry and execute RAFT to replicate
        transaction = Transaction(sender=sender, receiver=receiver, amount=amount)
        new_log_entry = LogEntry(term=self.current_term, transaction=transaction, is_2PC=is_2PC, tx_id=tx_id)
        self.log.append(new_log_entry)  # Append to leader log
        self.shardManager.append_to_log(new_log_entry)
        logger.info(f"Leader {self.my_address} appended new log entry: {transaction.__dict__}")

        # Send AppendEntries to all followers
        self.replicate_log()
    
    # def replicate_log(self):
    #     """Leader sends AppendEntries RPC to a follower starting from next_index[addr]."""
    #     for server in self.cluster_to_servers[self.my_cluster]:
    #         self.send_append_entries(server['addr'])

    #     # Wait for acknowledgments and retry if log inconsistency is detected
    #     start_time = time.time()
    #     acks_received = 1  # Leader counts itself

    #     while time.time() - start_time < 10:  # Wait for responses
    #         try:
    #             data, addr = self.socket.recvfrom(4096)
    #             response = json.loads(data.decode('utf-8'))
    #             logger.info(f"Received message from {addr}: {response}")

    #             if response.get("msg_type") == "APPEND_ACK":
    #                 if response.get("success"):
    #                     logger.info(f"Received ACK from {addr}")
    #                     acks_received += 1
    #                     self.next_index[addr] = len(self.log)  # Move next_index forward
    #                     self.match_index[addr] = self.next_index[addr] - 1
    #                     # logger.info(f"Match index for {addr}: {self.match_index[addr]}")
    #                     # logger.info(f"Next index for {addr}: {self.next_index[addr]}")

    #                     # If a majority has replicated, update commit index
    #                     if acks_received > 1:
    #                         logger.info(f"Majority reached with {acks_received} ACKs")
    #                         self.update_commit_index()
    #                         return
    #                 else:
    #                     logger.info(f"Log inconsistency detected with {addr}, decrementing next_index and retrying...")
    #                     self.next_index[addr] = max(0, self.next_index[addr] - 1)  # Move next_index back and retry
    #                     self.replicate_log()
    #                     return

    #         except socket.timeout:
    #             break  # Timeout, no majority reached

    def replicate_log(self):
        """Leader sends AppendEntries RPC to a follower and waits for majority acknowledgment."""
        self.replication_mode = True  # Enter replication mode

        for server in self.cluster_to_servers[self.my_cluster]:
            self.send_append_entries(server['addr'])

        acks_received = 1  # Leader counts itself
        start_time = time.time()

        while time.time() - start_time < 10:  # Wait for responses
            try:
                message, addr = self.append_ack_queue.get(timeout=1)
                logger.info(f"Replication mode: Received APPEND_ACK from {addr}: {message}")

                if message.get("success"):
                    self.next_index[addr] = len(self.log)
                    self.match_index[addr] = self.next_index[addr] - 1

                    match_count = sum(1 for index in self.match_index.values() if index >= self.commit_index)
                    if match_count > len(self.cluster_to_servers[self.my_cluster]) // 2:
                        logger.info(f"Majority of {match_count} reached, updating commit index.")
                        self.update_commit_index()
                        break

                else:
                    logger.info(f"Log inconsistency with {addr}, retrying...")
                    self.next_index[addr] = max(0, self.next_index[addr] - 1)
                    self.send_append_entries(addr)

            except queue.Empty:
                logger.info("Timeout waiting for APPEND_ACKs")
                break

        self.replication_mode = False  # Exit replication mode

    def update_commit_index(self):
        """Marks log entries as committed if stored on a majority of servers and at least one from the current term."""
        for index in range(len(self.log) - 1, self.commit_index, -1):  # Iterate backward from last entry to commit_index
            # find number of servers with log entry >= index (excluding leader)
            match_count = sum(1 for addr in self.match_index if self.match_index[addr] >= index)

            # If a majority of servers have this entry and it's from the current term, commit it
            if match_count > 0 and self.log[index].term == self.current_term:
                self.commit_index = index
                logger.info(f"{self.my_address} updated commit index to {self.commit_index}")

                log_entry = self.log[index]
                # If this is a 2PC transaction, send "VOTE YES" now (after replication but before execution)
                if log_entry.is_2PC and log_entry.tx_id:
                    with self.pending_decisions_lock:
                        logger.info(f"tx_id: {log_entry.tx_id}, sender: {log_entry.transaction.sender}, receiver: {log_entry.transaction.receiver}")
                        self.pending_decisions[log_entry.tx_id] = (time.time(), [log_entry.transaction.sender, log_entry.transaction.receiver])
                        logger.info(f"pending_decisions: {self.pending_decisions}")
                        
                    vote = Vote(tx_id=log_entry.tx_id, vote="yes").to_dict()
                    logger.info(f"Sending VOTE YES for cross-shard transaction: {log_entry.transaction}")
                    self.send_message(vote, self.coordinator_addr)
                    # Don't execute yet, wait for commit/abort decision
                    return
                logger.info(f"{self.my_address} committed log entries up to index {self.commit_index}")
                self.apply_committed_entries()
                return

    def apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        logger.info(f"applying entries up to index {self.commit_index}")
        # Apply transactions from the log that have not been applied to state machine yet
        for i in range(self.last_applied + 1, self.commit_index + 1):
            UPDATE_LAST_APPLIED = True
            log_entry = self.log[i]
            logger.info(f"i: {i}")

            if log_entry.transaction:
                # Handle the case where transaction is a dict instead of a Transaction object
                # if transaction == None it should be skipped by "if i == decision_entry_index:""
                if isinstance(log_entry.transaction, dict):
                    sender=log_entry.transaction["sender"]
                    receiver=log_entry.transaction["receiver"]
                    amount=log_entry.transaction["amount"]
                else:
                    sender=log_entry.transaction.sender
                    receiver=log_entry.transaction.receiver
                    amount=log_entry.transaction.amount


            # 2PC Transaction: Skip execution if not yet committed
            if log_entry.is_2PC:
                decision_entry_index, decision_entry = next(
                    (
                        (index, entry) 
                        for index, entry in enumerate(self.log) 
                        if entry.is_2PC and entry.tx_id == log_entry.tx_id and entry.transaction is None
                    ),
                    (None, None)
                )
                if decision_entry_index:
                    logger.info(f"decision entry index: {decision_entry_index} decision entry: {decision_entry.to_dict()}")
                
                if i == decision_entry_index:
                    logger.info("SKIPPING")
                    continue
                
                if not decision_entry:
                    logger.info(f"Skipping execution for 2PC transaction (tx_id: {log_entry.tx_id}), waiting for COMMIT/ABORT decision.")
                    UPDATE_LAST_APPLIED = False
                    continue
                if not decision_entry.committed_2PC:
                    logger.info(f"2PC transaction (tx_id: {log_entry.tx_id}) was ABORTED. Unlocking accounts and skipping execution.")
                    # Unlock accounts and do NOT execute
                    if self.shardManager.is_account_in_cluster(sender):
                        self.locks[sender] = False
                        logger.info(f"Unlocked sender {sender}")
                    elif self.shardManager.is_account_in_cluster(receiver):
                        self.locks[receiver] = False
                        logger.info(f"Unlocked receiver {receiver}")
                    continue

            # transaction = log_entry.transaction # Get transaction from log
            logger.info(f"Applying transaction: {(sender, receiver, amount)}")
            self.shardManager.execute_transaction((sender, receiver, amount), self.commit_index)

            logger.info(f"{self.my_address} executed transaction: {sender} sent ${amount} to {receiver}")

            # Leader notifies client
            if not log_entry.is_2PC:
                if self.role == "leader" and self.coordinator_addr:
                    response = ClientResponse(success=True, sender=sender, receiver=receiver, amount=amount)
                    self.send_message(vars(response), self.coordinator_addr)
            
            # Unlock sender and receiver
            logger.info(f"i: {i}, log[i]: {self.log[i]}")
            if not log_entry.is_2PC:
                self.locks[sender] = False
                self.locks[receiver] = False
                logger.info(f"Unlocked sender {sender} and receiver {receiver}")
            elif log_entry.is_2PC:
                if self.shardManager.is_account_in_cluster(sender):
                    # unlock sender
                    self.locks[sender] = False
                    logger.info(f"Unlocked sender {sender}")
                elif self.shardManager.is_account_in_cluster(receiver):
                    # unlock receiver
                    self.locks[receiver] = False
                    logger.info(f"Unlocked receiver {receiver}")
            
            if log_entry.is_2PC and decision_entry_index == self.commit_index:
                break

        if UPDATE_LAST_APPLIED:
            self.last_applied = self.commit_index  # Update last applied index
                    

    def handle_decision(self, message, addr):
        """Handles COMMIT/ABORT decision for a cross-shard (2PC) transaction."""
        tx_id = message.get("tx_id")
        decision = message.get("msg_type")  # Either "COMMIT" or "ABORT"

        logger.info(f"Received {decision} decision for cross-shard transaction with tx_id: {tx_id}")
        if not tx_id:
            logger.info("Error: Received COMMIT/ABORT message without tx_id. Ignoring.")
            return

        # Find the corresponding log entry]
        logger.info("Finding the corresponding log entry...")
        for log_entry in self.log:
            logger.info(f"log_entry : {log_entry.to_dict()}")
            logger.info(f"tx_id {tx_id} in self.pending_decisions : {log_entry.tx_id in self.pending_decisions}")
            logger.info(f"self.pending_decisions: {self.pending_decisions}")
            if log_entry.is_2PC and log_entry.tx_id == tx_id and log_entry.transaction:
                # Append a decision log entry for COMMIT or ABORT
                decision_entry = LogEntry(
                    term=self.current_term,
                    transaction=None,  # No transaction
                    is_2PC=True,
                    tx_id=tx_id,
                    committed_2PC=(decision == "COMMIT")  # True for COMMIT, False for ABORT
                )
                self.log.append(decision_entry)
                self.shardManager.append_to_log(decision_entry)
                logger.info(f"Appended {decision} log entry for tx_id {tx_id}")
                #logger.info(f"log entry: {log_entry.to_dict()}")
                logger.info(f"self.pending_decisions_lock: {self.pending_decisions_lock}")
                logger.info(f"self.pending_decisions: {self.pending_decisions}")
                with self.pending_decisions_lock:
                    del self.pending_decisions[tx_id]
                self.commit_index = len(self.log) - 1 # FIXME: ??????
                self.apply_committed_entries()

        # Send acknowledgment back to the client (coordinator)
        ack_message = Ack(tx_id=tx_id).to_dict()
        self.send_message(ack_message, addr)

    
    def handle_append_ack(self, message, addr):
        """Handle incoming AppendAck responses to maintain log consistency/repair."""
        success = message.get("success", False)

        if self.replication_mode:
            logger.info(f"Replication mode active: Queueing APPEND_ACK from {addr}")
            self.append_ack_queue.put((message, addr))
            return

        if success:
            logger.info(f"Log repair: Received successful APPEND_ACK from {addr}")
            self.next_index[addr] = len(self.log)
            self.match_index[addr] = self.next_index[addr] - 1
            # Check for majority replication and update commit index if applicable
            match_count = sum(1 for index in self.match_index.values() if index >= self.commit_index)
            if match_count > len(self.cluster_to_servers[self.my_cluster]) // 2:
                self.update_commit_index()
        else:
            logger.info(f"Log repair: Log inconsistency detected with {addr}, initiating repair...")
            self.next_index[addr] = max(0, self.next_index[addr] - 1)
            self.send_append_entries(addr)

    
    # def handle_append_ack(self, message, addr):
    #     """Handle incoming AppendAck responses to maintain log consistency/repair."""
    #     success = message.get("success", False)

    #     if success:
    #         logger.info(f"Received successful APPEND_ACK from {addr}")
    #         # Move next index forward and update match index
    #         self.next_index[addr] = len(self.log)
    #         self.match_index[addr] = self.next_index[addr] - 1
    #         logger.info(f"Match index for {addr}: {self.match_index[addr]}")
    #         logger.info(f"Next index for {addr}: {self.next_index[addr]}")

    #         # Check for majority replication and update commit index if applicable
    #         match_count = sum(1 for index in self.match_index.values() if index >= self.commit_index)
    #         if match_count > len(self.cluster_to_servers[self.my_cluster]) // 2:
    #             self.update_commit_index()

    #     else:
    #         logger.info(f"Log inconsistency detected with {addr}, initiating log repair...")
    #         # Decrement next index and retry log replication
    #         self.next_index[addr] = max(0, self.next_index[addr] - 1)
    #         self.send_append_entries(addr)

    
    def listen(self):
        # Listen for incoming UDP messages
        logger.info(f"Listening on {self.my_address[0]}:{self.my_address[1]}")
        while self.running:
            try:
                # set timeout to periodically check running flag
                self.socket.settimeout(1)
                data, addr = self.socket.recvfrom(2048) # receive message
                message_data = json.loads(data.decode('utf-8')) # decode message

                msg_type = message_data.get("msg_type")
                
                logger.info(f"{RED}[R]{RESET} {self.SERVER_NAMES[addr]} {msg_type}")

                # RAFT message handling
                if msg_type == "APPEND_ENTRIES":
                    self.handle_append_entries(message_data, addr)
                elif msg_type == "REQUEST_VOTE":
                    self.handle_vote_request(message_data, addr)
                elif msg_type == "CLIENT_REQUEST":
                    self.handle_client_request(message_data, addr)  # Process client request
                elif msg_type == "APPEND_ACK":
                    self.handle_append_ack(message_data, addr)


                # 2PC message handling
                elif msg_type == "PREPARE":
                    # 2PC Prepare phase
                    self.handle_prepare(message_data, addr)
                elif msg_type in ["COMMIT", "ABORT"]:
                    # 2PC Commit/Abort phase (final execution decision from client)
                    self.handle_decision(message_data, addr)

            except socket.timeout:
                # If no leader heartbeat is received, start an election
                if self.role == "follower" and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
    
    def _send(self, serialized_message, receiver, server_name, message):
        time.sleep(0.1)
        self.socket.sendto(serialized_message, receiver)
        msg_type = message.get('msg_type', 'UNKNOWN')
        logger.info(f"[T] {server_name} {msg_type}\n{json.dumps(message, indent=2)}")

    def send_message(self, message, receiver):
        serialized_message = json.dumps(message).encode('utf-8')
        server_name = self.SERVER_NAMES[receiver]
        
        threading.Thread(target=self._send, args=(serialized_message, receiver, server_name, message)).start()

    def clustercast(self, message, cluster):
        serialized_message = json.dumps(message).encode('utf-8')
        
        for server_info in self.cluster_to_servers[self.my_cluster]:
            threading.Thread(target=self._send, args=(serialized_message, server_info['addr'], server_info['id'], message)).start()

    def send_append_entries(self, addr):
        if addr not in self.next_index:
            self.next_index[addr] = len(self.log)  # Initialize next_index for new leader

        prev_log_index = self.next_index[addr] - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

        entries = [{
            "term": entry.term,
            "transaction": vars(entry.transaction) if isinstance(entry.transaction, Transaction) else None, 
            "is_2PC": entry.is_2PC,
            "tx_id": entry.tx_id,
            "committed_2PC": entry.committed_2PC
        } for entry in self.log[self.next_index[addr]:]]
        message = AppendEntries(
            term=self.current_term,
            leader_id=self.my_address,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index
        ).to_dict()

        self.send_message(message, addr)
        logger.info(f"Leader {self.my_address} sent AppendEntries RPC to {addr} with {len(entries)} entries.")
    
    def run(self):
        threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.process_transaction_queue, daemon=True).start()

        while self.running:
            time.sleep(1)

def main():
    if len(sys.argv) < 3:
        logger.info("Usage: python3 server.py <my_port> <cluster>")
        logger.info("Example: python3 server.py 5000 1")
        sys.exit(1)

    with open("config.json", "r") as f:
        config = json.load(f)
    
    coordinator_addr = (config["coordinator"]["ip"], config["coordinator"]["port"])

    my_port = int(sys.argv[1])
    my_cluster = int(sys.argv[2])

    server = Server("127.0.0.1", my_port, my_cluster, config['servers'], coordinator_addr)
    server.run()

if __name__ == "__main__":
    main()