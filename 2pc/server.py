# server.py
import sys
import json
import time
import random
import queue
from udp_messenger import UDPMessenger
from shard_manager import ShardManager
from message import *
import threading

HEARTBEAT_INTERVAL = 3  # Heartbeat interval (seconds)
ELECTION_TIMEOUT_RANGE = (3, 6)  # Election timeout range (seconds)
TRANSACTION_TIMEOUT = 5  # Timeout for acquiring locks (seconds)
SHARD_SIZE = 1000  # Number of accounts per shard

class LogEntry:
    """Class representing a log entry."""
    def __init__(self, term, transaction):
        self.term = term
        self.transaction = transaction

class Transaction:
    """Class representing a transaction."""
    def __init__(self, sender, receiver, amount):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

class Server:
    def __init__(self, id, cluster_id, messenger, coordinator_addr):
        self.messenger = messenger
        self.id = id
        self.cluster_id = cluster_id
        self.coordinator_addr = coordinator_addr
        self.prepared_transactions = {}  # {tx_id: data}

        self.shard_mgr = ShardManager(self.id, self.cluster_id)
        self.messenger.message_handler = self.handle_message

        # julia shit
        self.transaction_queue = queue.Queue()  # Queue for pending transactions
        self.running = True  # flag to control running state of listener thread
        self.shard_start = (cluster_id - 1) * 1000 + 1
        self.shard_end = cluster_id * 1000
        self.servers = self.messenger.cluster_to_servers[cluster_id] # {"id": server_id, "addr": (addr,port)}

        # RAFT variables
        self.current_term = 0
        self.current_leader = None
        self.voted_for = None
        self.log = []  # Transaction log
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = -1
        self.next_index = {} # Index of the next log entry to send to each follower
        self.match_index = {} # Index of the highest log entry known to be replicated on a server
        self.role = "follower"  # Initial state is follower
        self.election_timeout = random.uniform(*ELECTION_TIMEOUT_RANGE)  # Randomized timeout for leader election
        self.last_heartbeat = time.time()  # Track leader's last heartbeat
        self.election = False  # Flag to indicate if an election is in progress
        self.votes_received = 0  # Count of votes received during an election
        self.acks_received = 0  # Count of ACKs received during log replication
        self.replicate = False # Flag to indicate if log replication is in progress


    def handle_prepare(self, tx_id, data):
        """Vote Yes/No during Phase 1."""
        can_commit = self._can_commit(data)  # FIXME: julia use _can_commit to invoke RAFT here
        vote = "yes" if can_commit else "no"
        
        if can_commit:
            self.prepared_transactions[tx_id] = data
        
        vote_msg = Vote(tx_id=tx_id, vote=vote)
        self.messenger.send_message(vote_msg, self.coordinator_addr)

    def handle_decision(self, tx_id, decision):
        """Commit/Abort during Phase 2."""
        if decision == "COMMIT":
            self._commit(tx_id)
        else:
            self._abort(tx_id)
        
        ack_msg = Ack(tx_id=tx_id)
        self.messenger.send_message(ack_msg, self.coordinator_addr)

    def _commit(self, tx_id):
        if tx_id in self.prepared_transactions:
            # Persist data (your implementation)
            del self.prepared_transactions[tx_id]

    def _abort(self, tx_id):
        if tx_id in self.prepared_transactions:
            del self.prepared_transactions[tx_id]

    def _can_commit(self, data):
        # Example: Check if transaction is feasible
        return True  # Replace with your logic
    
    def handle_message(self, message, addr):
        msg_type = message.msg_type
        if msg_type != "CLIENT_REQUEST":
            term = message.term

        if msg_type == "CLIENT_REQUEST":
            print(f"\nReceived {msg_type} from {addr}")
        elif msg_type != "CLIENT_RESPONSE":
            print(f"\nReceived {msg_type} from {addr[1] % 1000}") # gets server id from port

        if self.election and self.role == "candidate":
            if term > self.current_term:
                self.step_down_to_follower(term)
            elif msg_type == "VOTE_RESPONSE" and term == self.current_term:
                    self.votes_received += 1
        
        if msg_type == "APPEND_ENTRIES":
            # AppendEntries RPC received from new leader: step down
            if term >= self.current_term and self.role == "candidate" and self.election:
                print(f"Received AppendEntries RPC from {message.leader_id}, stepping down to follower.")
                self.step_down_to_follower(term)
                self.last_heartbeat = time.time()  # Reset election timeout
            self.handle_append_entries(message, addr)
        elif msg_type == "REQUEST_VOTE":
            self.handle_vote_request(message, addr)
        elif msg_type == "CLIENT_REQUEST":
            self.handle_client_request(message, addr)  # Process client request
        elif msg_type == "APPEND_ACK":
            if message.success:
                print(f"Received ACK from {addr}")
                self.acks_received += 1
                self.next_index[addr] = len(self.log)  # Move next_index forward
                self.match_index[addr] = self.next_index[addr] - 1
                print(f"Match index for {addr}: {self.match_index[addr]}")
                print(f"Next index for {addr}: {self.next_index[addr]}")
            else:
                print(f"Log inconsistency detected with {addr}, decrementing next_index and retrying...")
                self.next_index[addr] = max(0, self.next_index[addr] - 1)  # Move next_index back and retry
                self.replicate_log(addr)
                self.replicate = False
        

    def step_down_to_follower(self, new_term):
        """Step down to follower upon receiving a higher term."""
        self.role = "follower"
        self.current_term = new_term
        self.voted_for = None
        self.election = False
        self.votes_received = 0

    def become_leader(self):
        """Transition to leader and start sending heartbeats."""
        self.role = "leader"
        self.election = False
        self.votes_received = 0 # reset votes
        print(f"{self.id} is now the leader for term {self.current_term}")
        # update next_index for all followers
        # for server in self.server_addresses:
        for server in self.servers:
            server_addr = server["addr"]
            self.next_index[server_addr] = len(self.log)
        # Start heartbeat thread
        threading.Thread(target=self.send_heartbeats, daemon=True).start()

    def check_needs_election(self):
        """Check if election is needed based on heartbeat timeout."""
        if self.role == "follower" and time.time() - self.last_heartbeat > self.election_timeout:
            self.start_election()
        # if no outcome, restart election
        if self.role == "candidate":
            election = False # ??
            time.sleep(random.uniform(2, 5)) # Prevent election collisions with random election delay
            self.start_election()

    def start_election(self):
        # Start a new election
        self.role = "candidate"
        self.election = True
        self.current_term += 1
        self.voted_for = self.id
        self.votes_received = 1  # Vote for self

        # Request votes from other servers
        message = RequestVote(
            term=self.current_term, 
            candidate_id=self.id, 
            last_log_index=len(self.log)-1, 
            last_log_term=self.log[-1].term if self.log else None
            ) # .to_dict()
        self.messenger.clustercast(message, self.cluster_id)
        # self.broadcast_message(message, "REQUEST_VOTE") # FIXME: change to clustercast


        # Wait for votes
        start_time = time.time()
        while (time.time() - start_time < self.election_timeout) and self.role == "candidate" and self.election: # FIXME: need election flag??
            # time.sleep(0.1)  # Sleep to avoid busy-waiting ???
            # Become leader if majority votes received
            # if self.votes_received > len(self.server_addresses) // 2: # Majority
            if self.votes_received > 1: # Majority
                self.become_leader()
                return  # Exit election loop
        
        # if no outcome, restart election
        # if self.role == "candidate":
        #     election = False # ??
        #     time.sleep(random.uniform(2, 5)) # Prevent election collisions with random election delay
        #     self.start_election()
                
    def send_heartbeats(self):
        """Send periodic heartbeats (empty AppendEntries RPC) to maintain authority."""
        while self.role == "leader":
            message = AppendEntries(
                term=self.current_term, 
                leader_id=self.id, 
                prev_log_index=len(self.log)-1, 
                prev_log_term=self.log[-1].term if self.log else None, 
                entries=[], 
                leader_commit=self.commit_index
                ) #.to_dict()
            
            print(f"HEARTBEAT")
            #self.broadcast_message(message, "APPEND_ENTRIES")
            self.messenger.clustercast(message, self.cluster_id)
            time.sleep(HEARTBEAT_INTERVAL) # heartbeat interval
            
            # If a higher term is received, leader should step down
            if self.role != "leader":
                print("Stepping down from leader role, stopping heartbeats.")
                break

    def handle_append_entries(self, message, addr):
        """Handle incoming AppendEntries RPC: heartbeats & log replication."""
        term = message.term
        leader_id = message.leader_id
        prev_log_index = message.prev_log_index
        prev_log_term = message.prev_log_term
        entries = message.entries
        leader_commit = getattr(message, "leader_commit", -1) # message.get("leader_commit", -1)

        if len(entries) == 0:
            print(f"HEARTBEAT from {leader_id} for term {term}")

        if prev_log_index is None:
            prev_log_index = -1
        if prev_log_term is None:
            prev_log_term = 0

        # Reject if term < current term
        if term < self.current_term:
            print(f"Received outdated RPC from leader {leader_id} for term (term {term} < {self.current_term}), ignoring.")
            return

        # Step down if term is higher
        if term > self.current_term:
            self.role = "follower"
            self.current_term = term
            self.voted_for = None # Reset vote
        
        self.last_heartbeat = time.time() # Reset election timeout

        # Update known leader on heartbeat
        if self.role == "follower":
            self.current_leader = leader_id
            # self.current_leader = tuple(leader_id)
            # print(f"Updated current leader to {leader_id}")

        # Reject if log doesn’t contain an entry at prev_log_index or term doesn't match
        # FIXME: Check if prev_log_index is -1????    
        if (prev_log_index != -1) and (prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term):
            print(f"Log mismatch at index {prev_log_index}, rejecting AppendEntries from {leader_id}")
            # FIXME: unlock accounts???
            response = AppendAck(success=False) #.to_dict()
            self.messenger.send_message(response, addr)
            # self.send_message(response, addr)
            return
        
        # If existing entries conflict with new entries, delete all existing entries starting with first conflicting entry
        if prev_log_index + 1 < len(self.log): # there are conflicting entries
            self.log = self.log[:prev_log_index + 1] # delete conflicting entries

        # Append any new entries not in log
        for entry in entries:
            sender = entry["transaction"]["sender"]
            receiver = entry["transaction"]["receiver"]
            print(f"sender: {sender}, receiver: {receiver}")
            print(f"entry: {entry}")
            transaction=entry["transaction"]
            # Create transaction object from dict
            if isinstance(transaction, dict):
                transaction = Transaction(transaction["sender"], transaction["receiver"], transaction["amount"])

            # Lock accounts on followers
            self.locks.setdefault(sender, False)
            self.locks.setdefault(receiver, False)
            self.locks[sender] = True
            self.locks[receiver] = True

            self.log.append(LogEntry(term=entry["term"], transaction=transaction))
            print(f"Appended new log entry from leader {leader_id}: term: {term}, {entry['transaction']}")

        # Update commit index
        if leader_commit > self.commit_index:
            self.commit_index = min(leader_commit, len(self.log) - 1)

            # Advance state machine with newly committed entries
            if self.commit_index > -1:
                self.apply_committed_entries()

        # Send ACK to leader
        response = AppendAck(success=True) #.to_dict()
        self.messenger.send_message(response, addr)
        #self.send_message(response, addr)

        print(f"commit index: {self.commit_index}")

                
    def handle_vote_request(self, message, addr):
        """Handle incoming RequestVote RPC."""
        term = message.term
        candidate_id = message.candidate_id
        last_log_index = message.last_log_index
        last_log_term = message.last_log_term
        
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
            print(f"Already voted for {self.voted_for} in term {self.current_term}, rejecting {candidate_id}")
            return
        
        # Reject if candidate's log is less complete
        my_last_log_index = len(self.log) - 1
        my_last_log_term = self.log[-1].term if self.log else 0
        if (last_log_term < my_last_log_term) or (last_log_term == my_last_log_term and last_log_index < my_last_log_index):
            print(f"Rejecting vote request from {candidate_id} (outdated log).")
            return

        # Grant vote
        self.voted_for = candidate_id
        response = VoteResponse(term=self.current_term, vote_granted=True)# .to_dict()
        self.messenger.send_message(response, addr)
        # self.send_message(response, addr)
        print(f"Voted for {candidate_id} in term {term}")

    def enqueue_transaction(self, message, addr):
        """Adds a transaction request to the queue."""
        self.transaction_queue.put((message, addr))  # Add transaction to queue
        print(f"[{self.id}] Successfully added transaction to queue: {message}")

    def process_transaction_queue(self):
        """Continuously processes transactions from the queue."""
        while True:
            try:
                message, addr = self.transaction_queue.get()  # get next transaction and dequeue
                print(f"[{self.id}] Processing transaction from queue: {message}")
                self.process_transaction(message, addr)  # Process transaction normally
            except Exception as e:
                print(f"[{self.id}] Error in transaction queue processing: {e}")
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
                current_leader_addr = self.servers[self.current_leader]["addr"]
                print(f"Redirecting client request to leader at {self.current_leader}")
                self.messenger.send_message(message, current_leader_addr)
                # self.send_message(message, self.current_leader)
            else:
                self.start_election()
                print("Error: No known leader to forward request.")

    def process_transaction(self, message, addr):
        """Processes a single transaction request."""
        """Handles intra-shard client request by adding a new log entry and replicating it to followers."""
        sender = int(message.sender)
        receiver = int(message.receiver)
        amount = int(message.amount)
        print(f"Received client request: {sender} sends ${amount} to {receiver}")
        
        # Check if sender has sufficient balance
        if self.data_store[sender] < amount:
            print(f"Transaction rejected: {sender} has insufficient balance.")
            return
        
        # Wait until both accounts are unlocked
        start_time = time.time()
        while self.locks[sender] or self.locks[receiver]:
            print(f"Waiting for {sender} and {receiver} to unlock...")
            if time.time() - start_time > TRANSACTION_TIMEOUT:
                print(f"Transaction rejected: Timeout while waiting for {sender} or {receiver} to unlock.")
                return  # Abort the transaction
            time.sleep(0.1)  # Short delay
        
        # Conditions met: lock both sender and receiver
        self.locks[sender] = True
        self.locks[receiver] = True

        # Create log entry and execute RAFT to replicate
        transaction = Transaction(sender=sender, receiver=receiver, amount=amount)
        new_log_entry = LogEntry(term=self.current_term, transaction=transaction)
        self.log.append(new_log_entry)  # Append to leader log
        print(f"Leader {self.id} appended new log entry: {transaction.__dict__}")

        # Send AppendEntries to all followers
        self.replicate_log()


    def replicate_log(self):
        """Leader sends AppendEntries RPC to a follower starting from next_index[addr]."""
        # for server in self.server_addresses:
        for server in self.servers:
            server_addr = server["addr"]
            self.send_append_entries(server_addr)

        # Wait for acknowledgments and retry if log inconsistency is detected
        start_time = time.time()
        self.acks_received = 1  # Leader counts itself
        self.replicate = True

        while (time.time() - start_time < 7) and self.replicate:  # Wait 3 seconds for responses
            # If a majority has replicated, update commit index
            # wait???
            # if self.acks_received > len(self.server_addresses) // 2:
            if self.acks_received > 1:
                print(f"Majority reached with {self.acks_received} ACKs")
                self.update_commit_index()
                return

    def send_append_entries(self, addr):
        """Leader sends AppendEntries RPC to a follower starting from next_index[addr]."""
        if addr not in self.next_index:
            self.next_index[addr] = len(self.log)  # Initialize next_index for new leader

        prev_log_index = self.next_index[addr] - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

        # Send all missing log entries starting from next_index
        entries = [{"term": entry.term, "transaction": vars(entry.transaction)} for entry in self.log[self.next_index[addr]:]]
        message = AppendEntries(
            term=self.current_term,
            leader_id=self.id,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index
        ) #.to_dict()

        self.messenger.send_message(message, addr)
        # self.send_message(message, addr)
        print(f"Leader {self.id} sent AppendEntries RPC to {addr} with {len(entries)} entries.")

    def update_commit_index(self):
        """Marks log entries as committed if stored on a majority of servers and at least one from the current term."""
        for index in range(len(self.log) - 1, self.commit_index, -1):  # Iterate backward from last entry to commit_index
            # find number of servers with log entry >= index (excluding leader)
            match_count = sum(1 for addr in self.match_index if self.match_index[addr] >= index)

            # If a majority of servers have this entry and it's from the current term, commit it
            # if match_count > (len(self.server_addresses) - 1) // 2 and self.log[index].term == self.current_term:
            if match_count > 0 and self.log[index].term == self.current_term:
                self.commit_index = index
                print(f"{self.id} updated commit index to {self.commit_index}")
                self.apply_committed_entries()
                print(f"Leader {self.id} committed log entries up to index {self.commit_index}")
                return

    def apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        print(f"{self.id} committing entries up to index {self.commit_index}")
        try:
            # Apply transactions from the log that have not been applied to state machine yet
            for i in range(self.last_applied + 1, self.commit_index + 1):
                transaction = self.log[i].transaction # Get transaction from log
                print(f"Applying transaction: {transaction}")
                sender, receiver, amount = transaction.sender, transaction.receiver, transaction.amount

                # Update balances in data store
                self.data_store.setdefault(sender, 0)
                self.data_store.setdefault(receiver, 0)
                self.data_store[sender] -= amount
                self.data_store[receiver] += amount
                print(f"{self.id} executed transaction: {sender} sent ${amount} to {receiver}")

                # Leader notifies client
                if self.role == "leader" and self.coordinator_addr:
                    response = ClientResponse(success=True, sender=sender, receiver=receiver, amount=amount)
                    # Send cleint response
                    self.send_client_response(vars(response), self.coordinator_addr)
                
            # Unlock sender and receiver
            self.locks[sender] = False
            self.locks[receiver] = False
            print(f"Unlocked sender {sender} and receiver {receiver}")

            self.last_applied = self.commit_index  # Update last applied index

            print(f"{self.id} Account Balances: sender {sender}: {self.data_store[sender]}, receiver {receiver}: {self.data_store[receiver]}")

        finally:
            print(f"Releasing locks for {sender} and {receiver}")
            self.locks[sender] = False
            self.locks[receiver] = False

    def send_client_response(self, response, receiver):
        # Send response to client
        self.messenger.send_message(response, receiver)
        print(f"Leader {self.id} sent response to client: {response}")

    def run(self):
        # Start listening thread
        # threading.Thread(target=self.listen, daemon=True).start()
        threading.Thread(target=self.check_needs_election, daemon=True).start()
        threading.Thread(target=self.process_transaction_queue, daemon=True).start()

        while self.running:
            time.sleep(1)

def main():
    # Check for correct number of arguments
    if len(sys.argv) < 4:
        print("Usage: python3 server.py <my_port> <my_id> <my_cluster>")
        sys.exit(1)

    # Load config
    with open("config.json", "r") as f:
        config = json.load(f)
    
    coordinator_addr = (config["coordinator"]["ip"], config["coordinator"]["port"])

    # Parse command-line arguments
    my_port = int(sys.argv[1])
    my_id = int(sys.argv[2])
    my_cluster = int(sys.argv[3])

    # Initialize UDP messenger
    messenger = UDPMessenger(
        my_ip="127.0.0.1",
        my_port=my_port,
        server_config=config['servers'],
        log_level="info"
    )

    # Run server
    server = Server(my_id, my_cluster, messenger, coordinator_addr)
    print(f"Running as Server on port {my_port}, ID {my_id}, Cluster {my_cluster}...")
    while True:
        server.run()
        # # Keep the server running
        # time.sleep(1)  # Sleep to avoid busy-waiting

if __name__ == "__main__":
    main()
