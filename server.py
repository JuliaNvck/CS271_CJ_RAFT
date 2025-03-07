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

DECISION_TIMEOUT = 15
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
        self.applied_log_indices = set()


        # Initialize data store for transactions
        self.data_store = {id: 10 for id in range(self.shard_start, self.shard_end + 1)}
        # Tracks which accounts are locked
        self.locks = {id: False for id in range(self.shard_start, self.shard_end + 1)}
        
        self.pending_decisions = {}  # Track pending 2PC transactions {tx_id: (timestamp, transaction)}
        self.timed_out_transactions = set()
        # self.pending_decisions_lock = threading.Lock()  # To prevent race conditions
        threading.Thread(target=self.check_pending_decisions, daemon=True).start()

    def check_pending_decisions(self):
        while True:
            current_time = time.time()
            for tx_id, (start_time, transaction) in list(self.pending_decisions.items()):
                if current_time - start_time > DECISION_TIMEOUT:
                    logger.info(f"Transaction {tx_id} timed out after {DECISION_TIMEOUT}s")
                    
                    # Mark this transaction as timed out in your transaction state
                    self.timed_out_transactions.add(tx_id)
                    
                    # Release the locks
                    self.release_locks_for_transaction(tx_id)
                    
                    # Remove from pending decisions
                    self.pending_decisions.pop(tx_id, None)
                    
                    # Send an ABORT message to the coordinator
                    ack_message = Ack(tx_id=tx_id).to_dict()
                    self.send_message(ack_message, self.coordinator_addr)
            time.sleep(0.3)

    def release_locks_for_transaction(self, tx_id):
        _, accounts = self.pending_decisions.get(tx_id, (None, []))
        
        # Ensure 'accounts' is a flat list of integers
        for account in accounts:
            if isinstance(account, list):
                for acc in account:
                    if self.shardManager.is_account_in_cluster(acc):
                        self.locks[acc] = False
                        logger.info(f"Unlocked account {acc}")
            elif isinstance(account, int):
                if self.shardManager.is_account_in_cluster(account):
                    self.locks[account] = False
                    logger.info(f"Unlocked account {account}")
    
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
        prev_log_index = message.get("prev_log_index", -1)  # Default to -1 if None
        prev_log_term = message.get("prev_log_term", 0)     # Default to 0 if None
        entries = message.get("entries", [])
        leader_commit = message.get("leader_commit", -1)

        # Log heartbeats
        if len(entries) == 0:
            logger.info(f"HEARTBEAT from {leader_id} for term {term}")

        # Step 1: Validate term
        if term < self.current_term:
            logger.info(f"Received outdated RPC from leader {leader_id} (term {term} < {self.current_term}), ignoring.")
            response = AppendAck(success=False).to_dict()
            self.send_message(response, addr)
            return

        # Step down if term is higher
        if term > self.current_term:
            self.role = "follower"
            self.current_term = term
            self.voted_for = None  # Reset vote
        
        # Reset election timeout on valid AppendEntries
        self.last_heartbeat = time.time()

        # Update known leader
        if self.role == "follower":
            self.current_leader = tuple(leader_id)

        # Step 2: Check log consistency
        # Validate prev_log_index is within bounds
        if prev_log_index >= len(self.log):
            logger.warning(f"Invalid prev_log_index {prev_log_index}. Current log length: {len(self.log)}.")
            response = AppendAck(success=False).to_dict()
            self.send_message(response, addr)
            return
        
        # Check if previous log entry term matches
        if prev_log_index >= 0 and (self.log[prev_log_index].term != prev_log_term):
            logger.info(f"Log term mismatch at index {prev_log_index}, rejecting AppendEntries from {leader_id}")
            response = AppendAck(success=False).to_dict()
            self.send_message(response, addr)
            return
        
        # Step 3: Handle log conflicts
        # If we have entries after prev_log_index, check for conflicts and truncate if needed
        if entries and prev_log_index + 1 < len(self.log):
            logger.info(f"Potential log conflict. Current log length = {len(self.log)}")
            # Delete conflicting entries
            self.log = self.log[:prev_log_index + 1]
            self.shardManager.truncate_log(prev_log_index + 1)
            logger.info(f"Deleted conflicting log entries. New log length = {len(self.log)}")

        # Step 4: Append new entries
        for entry in entries:
            is_2PC = entry.get("is_2PC", False)
            transaction = entry.get("transaction")
            tx_id = entry.get("tx_id")
            committed_2PC = entry.get("committed_2PC", None)
            
            # Log transaction details
            if transaction:
                sender = transaction.get("sender")
                receiver = transaction.get("receiver")
                logger.info(f"Processing transaction - sender: {sender}, receiver: {receiver}, tx_id: {tx_id}")
            
            # Check if this is a decision update for an existing 2PC transaction
            # if tx_id is not None:
            #     existing_entry = next((e for e in self.log if hasattr(e, 'tx_id') and e.tx_id == tx_id), None)
            #     if existing_entry and committed_2PC and not existing_entry.committed_2PC:
            #         logger.info(f"Updating committed_2PC status for tx_id: {tx_id} to {committed_2PC}")
            #         existing_entry.committed_2PC = committed_2PC
            #         continue  # Skip adding a new entry
            
            # Handle locks based on transaction type
            if transaction:
                if isinstance(transaction, dict):
                    sender = transaction.get("sender")
                    receiver = transaction.get("receiver")
                    amount = transaction.get("amount")
                    
                    # Create Transaction object for non-2PC transactions
                    if not is_2PC:
                        transaction = Transaction(sender, receiver, amount)
                    
                    # Acquire locks based on transaction type
                    if is_2PC:
                        # For 2PC, only lock accounts in our shard
                        if self.shardManager.is_account_in_cluster(sender):
                            self.locks[sender] = True
                            self.pending_decisions[tx_id] = (time.time(), [sender, receiver])
                            logger.info(f"Acquired lock on sender {sender} for 2PC tx_id: {tx_id}")
                        elif self.shardManager.is_account_in_cluster(receiver):
                            self.locks[receiver] = True
                            self.pending_decisions[tx_id] = (time.time(), [sender, receiver])
                            logger.info(f"Acquired lock on receiver {receiver} for 2PC tx_id: {tx_id}")
                    else:
                        # For intra-shard, lock both accounts
                        self.locks[sender] = True
                        self.locks[receiver] = True
                        logger.info(f"Acquired locks on {sender} and {receiver} for intra-shard transaction")
            
            # Create and append the log entry
            log_entry = LogEntry(
                term=entry.get("term", self.current_term),
                transaction=transaction,
                is_2PC=is_2PC,
                tx_id=tx_id,
                committed_2PC=committed_2PC
            )
            
            self.log.append(log_entry)
            self.shardManager.append_to_log(log_entry)
            logger.info(f"Appended new log entry: term={log_entry.term}, is_2PC={is_2PC}, tx_id={tx_id}")

        # Step 5: Update commit index and apply committed entries
        if leader_commit > self.commit_index:
            prev_commit = self.commit_index
            self.commit_index = min(leader_commit, len(self.log) - 1)
            logger.info(f"Updated commit index from {prev_commit} to {self.commit_index}")
            
            # Apply newly committed entries
            if self.commit_index > prev_commit:
                self.apply_committed_entries()

        # Send success acknowledgment
        response = AppendAck(success=True).to_dict()
        self.send_message(response, addr)
        logger.info(f"Current commit index: {self.commit_index}")
                
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
        if self.shardManager.is_account_in_cluster(sender):
            print(f"Sender: {sender}, Balance: {self.shardManager.get_balance(sender)}")

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
                    self.send_message(ClientResponse(False, sender, receiver, amount).to_dict(), self.coordinator_addr)
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

    def replicate_log(self):
        """Leader sends AppendEntries RPC to all followers and waits for majority acknowledgment."""
        self.replication_mode = True  # Enter replication mode
        
        # Send AppendEntries to all servers in the cluster
        for server in self.cluster_to_servers[self.my_cluster]:
            if server['addr'] != self.my_address:  # Don't send to self
                self.send_append_entries(server['addr'])
        
        acks_received = 1  # Leader counts itself
        servers_responded = {self.my_address}  # Track which servers have responded
        
        # Get the total number of servers in the cluster
        total_servers = len(self.cluster_to_servers[self.my_cluster])
        majority = (total_servers // 2) + 1
        
        start_time = time.time()
        timeout = 5  # Reduced timeout to 5 seconds
        
        # Find the last entry in the log that needs replication
        last_entry_index = len(self.log) - 1
        
        while time.time() - start_time < timeout and acks_received < majority:
            try:
                message, addr = self.append_ack_queue.get(timeout=0.5)
                logger.info(f"Replication mode: Received APPEND_ACK from {addr}: {message}")
                
                if addr in servers_responded:
                    logger.info(f"Already received ACK from {addr}, ignoring duplicate")
                    continue
                
                if message.get("success"):
                    servers_responded.add(addr)
                    acks_received += 1
                    
                    # Update next and match indices for the follower
                    self.next_index[addr] = len(self.log)
                    self.match_index[addr] = len(self.log) - 1
                    
                    logger.info(f"Received successful ACK from {addr}. Total ACKs: {acks_received}/{majority} required")
                    
                    # Check if we've reached a majority
                    if acks_received >= majority:
                        logger.info(f"Majority reached ({acks_received}/{total_servers}), updating commit index")
                        self.update_commit_index()
                        break
                else:
                    # Log inconsistency, decrement next_index and retry
                    logger.info(f"Log inconsistency with {addr}, retrying with decreased index")
                    self.next_index[addr] = max(0, self.next_index[addr] - 1)
                    self.send_append_entries(addr)
                    
            except queue.Empty:
                logger.info("Timeout waiting for APPEND_ACKs, checking if we have majority")
                # Check if we already have a majority despite the timeout
                if acks_received >= majority:
                    logger.info(f"Already have majority ({acks_received}/{total_servers}), continuing")
                    self.update_commit_index()
                    break
        
        # Check if we failed to get a majority
        if acks_received < majority:
            logger.warning(f"Failed to reach majority ({acks_received}/{majority} required). Handling failure.")
            
            # Find non-committed transactions and send failure responses
            for index in range(self.commit_index + 1, len(self.log)):
                entry = self.log[index]
                
                # Only handle intra-shard transactions here
                if not entry.is_2PC and entry.transaction is not None:
                    # For intra-shard transactions, we'll abort if we can't reach consensus
                    logger.info(f"Aborting intra-shard transaction at index {index} due to failure to reach consensus")
                    
                    if isinstance(entry.transaction, dict):
                        sender = entry.transaction["sender"]
                        receiver = entry.transaction["receiver"]
                        amount = entry.transaction["amount"]
                    else:
                        sender = entry.transaction.sender
                        receiver = entry.transaction.receiver
                        amount = entry.transaction.amount
                    
                    # Unlock the data items
                    if sender in self.locks:
                        self.locks[sender] = False
                        logger.info(f"Unlocked sender {sender}")
                    if receiver in self.locks:
                        self.locks[receiver] = False
                        logger.info(f"Unlocked receiver {receiver}")
                    
                    # Send a failure response to the client for intra-shard transaction
                    if self.coordinator_addr:
                        logger.info(f"Sending failure response for transaction ({sender}, {receiver}, {amount})")
                        failure_response = ClientResponse(success=False, sender=sender, receiver=receiver, amount=amount)
                        self.send_message(vars(failure_response), self.coordinator_addr)
        
        self.replication_mode = False  # Exit replication mode
        return acks_received >= majority  # Return success status

    def update_commit_index(self):
        """Marks log entries as committed if stored on a majority of servers and at least one from the current term."""
        if len(self.log) == 0:
            logger.info("Log is empty, nothing to commit")
            return
            
        # Get the total number of servers in the cluster
        total_servers = len(self.cluster_to_servers[self.my_cluster])
        majority = (total_servers // 2) + 1
        
        # The leader counts itself in the match count
        leader_match_index = len(self.log) - 1
        
        # Find the highest index that's replicated on a majority of servers
        highest_committed_index = self.commit_index
        
        for index in range(len(self.log) - 1, self.commit_index, -1):
            # Count servers with this entry (including leader)
            match_count = 1  # Start with 1 for the leader
            
            for addr in self.match_index:
                if self.match_index[addr] >= index:
                    match_count += 1
            
            logger.info(f"Index {index} has {match_count}/{total_servers} matches (need {majority})")
            
            # Only commit entries from current term and if majority has it
            if match_count >= majority and self.log[index].term == self.current_term:
                highest_committed_index = index
                break
        
        # Update commit index if we found a new highest
        if highest_committed_index > self.commit_index:
            self.commit_index = highest_committed_index
            logger.info(f"{self.my_address} updated commit index to {self.commit_index}")
            
            # Check if this is a 2PC transaction
            log_entry = self.log[self.commit_index]
            
            if log_entry.is_2PC and log_entry.transaction is not None:
                # For 2PC transactions, we need to send a vote to the coordinator
                tx_id = log_entry.tx_id
                
                if isinstance(log_entry.transaction, dict):
                    sender = log_entry.transaction["sender"]
                    receiver = log_entry.transaction["receiver"]
                    amount = log_entry.transaction["amount"]
                else:
                    sender = log_entry.transaction.sender
                    receiver = log_entry.transaction.receiver
                    amount = log_entry.transaction.amount
                
                logger.info(f"Handling 2PC transaction with tx_id: {tx_id}, sender: {sender}, receiver: {receiver}")
                
                # Store the transaction in pending decisions
                self.pending_decisions[tx_id] = (time.time(), [sender, receiver])
                
                # Check if we can vote YES
                # We need to check if this participant can execute the transaction
                can_vote_yes = True
                
                if self.shardManager.is_account_in_cluster(sender):
                    # Check if sender has enough balance
                    balance = self.shardManager.get_balance(sender)
                    if balance < amount:
                        logger.info(f"Sender {sender} has insufficient balance ({balance} < {amount}). Voting NO.")
                        can_vote_yes = False
                
                # Send vote to coordinator
                if can_vote_yes:
                    vote = Vote(tx_id=tx_id, vote="yes").to_dict()
                    logger.info(f"Sending VOTE YES for tx_id: {tx_id}")
                else:
                    vote = Vote(tx_id=tx_id, vote="no").to_dict()
                    logger.info(f"Sending VOTE NO for tx_id: {tx_id}")
                    
                self.send_message(vote, self.coordinator_addr)
                
                # For 2PC, we don't apply the transaction yet, we wait for the coordinator's decision
                return
                
            # For intra-shard transactions or other entries, apply immediately
            self.apply_committed_entries()               

    def apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        logger.info(f"Entering apply_committed_entries...")
        logger.info(f"last_applied= {self.last_applied}")
        logger.info(f"commit_index= {self.commit_index}")
        logger.info(f"Range: {range(self.last_applied + 1, self.commit_index + 1)}")
        
        # Apply transactions from the log that have not been applied to state machine yet
        for i in range(self.last_applied + 1, self.commit_index + 1):
            logger.info(f"i: {i}")
            if i >= len(self.log):
                logger.warning(f"Cannot apply entry at index {i}, log length is {len(self.log)}")
                break
                
            # Prevent reapplying the same log entry using log index
            if i in self.applied_log_indices:
                logger.info(f"Log entry at index {i} already applied. Skipping to {i+1}...")
                continue
                
            log_entry = self.log[i]
            logger.info(f"Log entry {i}: {log_entry.to_dict()}")

            sender, receiver, amount = None, None, None
            if log_entry.transaction:
                # Handle the case where transaction is a dict instead of a Transaction object
                if isinstance(log_entry.transaction, dict):
                    sender = log_entry.transaction["sender"]
                    receiver = log_entry.transaction["receiver"]
                    amount = log_entry.transaction["amount"]
                else:
                    sender = log_entry.transaction.sender
                    receiver = log_entry.transaction.receiver
                    amount = log_entry.transaction.amount

            # 2PC Transaction: Find the decision entry for this transaction
            if log_entry.is_2PC:
                # If not committed, skip execution
                logger.info(f"log_entry.committed_2PC: {log_entry.committed_2PC}")
                if log_entry.committed_2PC is None:
                    logger.info(f"2PC transaction (tx_id: {log_entry.tx_id}) is pending. Skipping execution.")
                    continue
                elif not log_entry.committed_2PC:
                    logger.info(f"2PC transaction (tx_id: {log_entry.tx_id}) was ABORTED. Skipping execution.")

                     # Unlock accounts
                    if sender is not None and self.shardManager.is_account_in_cluster(sender):
                        self.locks[sender] = False
                        logger.info(f"Unlocked sender {sender}")
                    elif receiver is not None and self.shardManager.is_account_in_cluster(receiver):
                        self.locks[receiver] = False
                        logger.info(f"Unlocked receiver {receiver}")

                    self.applied_log_indices.add(i)
                    continue

            # Execute the transaction
            logger.info(f"Applying transaction: {(sender, receiver, amount)}")
            self.shardManager.execute_transaction((sender, receiver, amount), self.commit_index)
            self.applied_log_indices.add(i)
            logger.info(f"{self.my_address} executed transaction: {sender} sent ${amount} to {receiver}")

            # Leader notifies client for non-2PC transactions
            if not log_entry.is_2PC:
                if self.role == "leader" and self.coordinator_addr:
                    response = ClientResponse(success=True, sender=sender, receiver=receiver, amount=amount)
                    self.send_message(vars(response), self.coordinator_addr)
            
            # Unlock sender and receiver based on transaction type
            if not log_entry.is_2PC:
                # For regular transactions, unlock both accounts
                if sender in self.locks:
                    self.locks[sender] = False
                    logger.info(f"Unlocked sender {sender}")
                if receiver in self.locks:
                    self.locks[receiver] = False
                    logger.info(f"Unlocked receiver {receiver}")
            elif log_entry.is_2PC:
                # For 2PC, unlock only the account in this cluster
                if self.shardManager.is_account_in_cluster(sender):
                    self.locks[sender] = False
                    logger.info(f"Unlocked sender {sender}")
                elif self.shardManager.is_account_in_cluster(receiver):
                    self.locks[receiver] = False
                    logger.info(f"Unlocked receiver {receiver}")
        
        # Update last_applied to the highest applied index
        # if self.applied_log_indices:
        #     self.last_applied = max(self.applied_log_indices)
        # Update last_applied to the highest applied index
        if self.applied_log_indices:
            self.last_applied = max(self.applied_log_indices)
            # Sort the indices to ensure they are in order
            sorted_indices = sorted(self.applied_log_indices)
            
            # Initialize the last_applied to -1
            last_applied = -1
            
            # Iterate through the sorted indices to find the maximum contiguous sequence
            for val in sorted_indices:
                if val == last_applied + 1:
                    last_applied = val
                else:
                    break
            
            logger.info(f"Last applied= {last_applied}")
            self.last_applied = last_applied

    
    def handle_decision(self, message, addr):
        """Handles COMMIT/ABORT decision for a cross-shard (2PC) transaction."""
        tx_id = message.get("tx_id")
        decision = message.get("msg_type")  # Either "COMMIT" or "ABORT"

        logger.info(f"Received {decision} decision for cross-shard transaction with tx_id: {tx_id}")
        
        # Validate tx_id
        if not tx_id:
            logger.info("Error: Received COMMIT/ABORT message without tx_id. Ignoring.")
            return
        
        # Track processed decisions to prevent duplicate ACKs
        if not hasattr(self, 'processed_decisions'):
            self.processed_decisions = set()
            
        # Check if we've already processed this decision
        decision_key = f"{tx_id}_{decision}"
        if decision_key in self.processed_decisions:
            logger.info(f"Already processed {decision} for tx_id {tx_id}. Not sending duplicate ACK.")
            return
            
        # Mark this decision as processed
        self.processed_decisions.add(decision_key)
        
        # Check if this transaction already timed out
        if hasattr(self, 'timed_out_transactions') and tx_id in self.timed_out_transactions:
            logger.info(f"Ignoring late {decision} for already timed-out transaction {tx_id}")
            self.timed_out_transactions.remove(tx_id)  # Clean up
            return  # Don't process or send ACK

        # Find the corresponding log entry
        logger.info("Finding the corresponding log entry...")
        found_entry = False
        found_entry = False
        entry_index = -1
    
        for i, log_entry in enumerate(self.log):
            if hasattr(log_entry, 'is_2PC') and log_entry.is_2PC and hasattr(log_entry, 'tx_id') and log_entry.tx_id == tx_id and log_entry.transaction:
                found_entry = True
                entry_index = i
                
                # Update the committed_2PC flag directly on the transaction entry
                logger.info(f"Updating committed_2PC flag to {decision == 'COMMIT'} for tx_id {tx_id}")
                log_entry.committed_2PC = (decision == "COMMIT")
                
                # Update the same entry in the shard manager's log
                self.shardManager.updateLogEntryCommittedFlag(i, log_entry.committed_2PC)
                break
        
        # Clean up pending transactions
        if tx_id in self.pending_decisions:
            logger.info(f"Removing tx_id {tx_id} from pending decisions after {decision}")
            self.pending_decisions.pop(tx_id, None)
        
        # Apply committed entries (which will include new decision)
        self.apply_committed_entries()
        
        if not found_entry:
            logger.info(f"No matching log entry found for tx_id {tx_id}, but still added decision and releasing locks")
        
        # if decision != "COMMIT":
        #     # ABORT Release locks for this transaction
        #     self.release_locks_for_transaction(tx_id)
        #     logger.info(f"Transaction with tx_id {tx_id} was ABORTED. Releasing locks.")
        # Always send an ACK back to the client (coordinator)
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