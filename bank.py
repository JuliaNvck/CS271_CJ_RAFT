import socket
import threading
import sys
from collections import deque
import json
import hashlib
import heapq
import time
import random
from messages import RequestVote, VoteResponse, AppendEntries, ClientRequest, AppendAck, ClientResponse


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

# Predefined ports and IP addresses for the servers
DEFAULT_SERVERS = [
    ("127.0.0.1", 5000),  # Server 1
    ("127.0.0.1", 5001),  # Server 2
    ("127.0.0.1", 5002),   # Server 3
]

# Mapping of addresses to server identifiers (server 1, 2, 3)
SERVER_NAMES = {server: f"Server {i+1}" for i, server in enumerate(DEFAULT_SERVERS)}

# Server class (part of a cluster)
class Server:
    def __init__(self, my_ip, my_port, server_addresses, cluster_id):
        self.my_address = (my_ip, my_port) # initialize server with address
        self.server_addresses = server_addresses # list of other peer's addresses
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # udp socket
        self.socket.bind(self.my_address) # bind to UDP socket
        self.running = True  # flag to control running state of listener thread
        self.cluster_id = cluster_id
        self.shard_start = (cluster_id - 1) * 1000 + 1
        self.shard_end = cluster_id * 1000

        # RAFT variables
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Transaction log
        self.commit_index = -1  # Index of highest log entry known to be committed
        self.last_applied = 0
        self.next_index = {} # Index of the next log entry to send to each follower
        self.match_index = {} # Index of the highest log entry known to be replicated on a server
        self.role = "follower"  # Initial state is follower
        self.election_timeout = random.uniform(3, 6)  # Randomized timeout for leader election: [T, 2T]
        self.last_heartbeat = time.time()  # Track leader's last heartbeat
        self.client_addr = None  # Track client address

        # Initialize data store for transactions
        self.data_store = {id: 10 for id in range(self.shard_start, self.shard_end + 1)}
        # Tracks which accounts are locked
        self.locks = {id: False for id in range(self.shard_start, self.shard_end + 1)}


    def start_election(self):
        # Start a new election
        self.role = "candidate"
        self.current_term += 1
        self.voted_for = self.my_address
        votes_received = 1  # Vote for self

        # Request votes from other servers
        message = RequestVote(
            term=self.current_term, 
            candidate_id=self.my_address, 
            last_log_index=len(self.log)-1, 
            last_log_term=self.log[-1].term if self.log else None
            ).to_dict()
        self.broadcast_message(message, "REQUEST_VOTE")

        # Wait for votes
        start_time = time.time()
        while time.time() - start_time < self.election_timeout:
            try:
                data, addr = self.socket.recvfrom(4096)
                response = json.loads(data.decode('utf-8'))

                # Step down if a higher term message is received
                if response.get("term", 0) > self.current_term:
                    print(f"Received higher term {response.get('term')}, stepping down to follower.")
                    self.role = "follower"
                    self.current_term = response.get("term")
                    self.voted_for = None
                    return  # Stop election process
                
                # AppendEntries RPC received from new leader: step down
                if response.get("msg_type") == "APPEND_ENTRIES" and response.get("term", 0) >= self.current_term:
                    print(f"Received AppendEntries RPC from {response.get('leader_id')}, stepping down to follower.")
                    self.role = "follower"
                    self.current_term = response.get("term")
                    self.voted_for = None
                    self.last_heartbeat = time.time()  # Reset election timeout
                    return  # Stop election process
                
                # Count votes
                if response.get("msg_type") == "VOTE_RESPONSE" and response.get("term") == self.current_term:
                    votes_received += 1
                
                # Become leader if majority votes received
                if votes_received > len(self.server_addresses) // 2: # Majority
                    self.role = "leader"
                    print(f"{SERVER_NAMES[self.my_address]} is now the leader for term {self.current_term}")
                    # update next_index for all followers
                    for server in self.server_addresses:
                        self.next_index[server] = len(self.log)
                    # Start heartbeat thread
                    threading.Thread(target=self.send_heartbeats, daemon=True).start()
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
            
            self.broadcast_message(message, "APPEND_ENTRIES")
            time.sleep(3) # heartbeat interval
            
            # If a higher term is received, leader should step down
            if self.role != "leader":
                print("Stepping down from leader role, stopping heartbeats.")
                break

    def handle_append_entries(self, message, addr):
        """Handle incoming AppendEntries RPC: heartbeats & log replication."""
        term = message.get("term")
        leader_id = message.get("leader_id")
        prev_log_index = message.get("prev_log_index")
        prev_log_term = message.get("prev_log_term")
        entries = message.get("entries", [])
        leader_commit = message.get("leader_commit", 0)

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

       # Reject if log doesn’t contain an entry at prev_log_index or term doesn't match
        # FIXME: Check if prev_log_index is -1????    
        if (prev_log_index != -1) and (prev_log_index >= len(self.log) or self.log[prev_log_index].term != prev_log_term):
            print(f"Log mismatch at index {prev_log_index}, rejecting AppendEntries from {leader_id}")
            # FIXME: unlock accounts???
            response = AppendAck(success=False).to_dict()
            self.send_message(response, addr)
            return
        
        # If existing entries conflict with new entries, delete all existing entries starting with first conflicting entry
        if prev_log_index + 1 < len(self.log): # there are conflicting entries
            self.log = self.log[:prev_log_index + 1] # delete conflicting entries

        # Append any new entries not in log
        for entry in entries:
            sender = entry["transaction"]["sender"]
            receiver = entry["transaction"]["receiver"]

            # Lock accounts on followers
            self.locks.setdefault(sender, False)
            self.locks.setdefault(receiver, False)
            self.locks[sender] = True
            self.locks[receiver] = True

            self.log.append(LogEntry(term=entry["term"], transaction=entry["transaction"]))

        # Update commit index
        if message.get("leader_commit", -1) > self.commit_index:
            self.commit_index = min(message.get("leader_commit", -1), len(self.log) - 1)

        # Advance state machine with newly committed entries
        if self.commit_index > -1:
            self.apply_committed_entries()

        # Send ACK to leader
        response = AppendAck(success=True).to_dict()
        self.send_message(response, addr)

        print(f"Follower {self.my_address} commit index: {self.commit_index}")

                
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
        response = VoteResponse(term=self.current_term, vote_granted=True).to_dict()
        self.send_message(response, addr)
        print(f"Voted for {candidate_id} in term {term}")

    def handle_client_request(self, message, addr):
        """Handles intra-shard client request by adding a new log entry and replicating it to followers."""
        sender = message.get("sender")
        receiver = message.get("receiver")
        amount = message.get("amount")
        self.client_addr = addr  # Track client address

        # Check if sender has sufficient balance
        if self.data_store[sender] < amount:
            print(f"Transaction rejected: {sender} has insufficient balance.")
            return
        
         # Set a timeout limit (e.g., 5 seconds)
        timeout = 5  # seconds
        start_time = time.time()
        # Wait until both accounts are unlocked
        while self.locks[sender] or self.locks[receiver]:
            if time.time() - start_time > timeout:
                print(f"Transaction rejected: Timeout while waiting for {sender} or {receiver} to unlock.")
                return  # Abort the transaction
            
            print(f"Waiting: {sender} or {receiver} is locked.")
            time.sleep(0.1)  # Short delay
        
        # Conditions met: lock both sender and receiver
        self.locks[sender] = True
        self.locks[receiver] = True

        # Create log entry and execute RAFT to replicate
        transaction = Transaction(sender=sender, receiver=receiver, amount=amount)
        new_log_entry = LogEntry(term=self.current_term, transaction=transaction)
        self.log.append(new_log_entry)  # Append to leader log
        print(f"Leader {self.my_address} appended new log entry: {transaction}")

        # Send AppendEntries to all followers
        self.replicate_log()

    def send_append_entries(self, addr):
        """Leader sends AppendEntries RPC to a follower starting from next_index[addr]."""
        if addr not in self.next_index:
            self.next_index[addr] = len(self.log)  # Initialize next_index for new leader

        prev_log_index = self.next_index[addr] - 1
        prev_log_term = self.log[prev_log_index].term if prev_log_index >= 0 else 0

        # Send all missing log entries starting from next_index
        entries = [{"term": entry.term, "command": entry.command} for entry in self.log[self.next_index[addr]:]]
        message = AppendEntries(
            term=self.current_term,
            leader_id=self.my_address,
            prev_log_index=prev_log_index,
            prev_log_term=prev_log_term,
            entries=entries,
            leader_commit=self.commit_index
        ).to_dict()

        self.send_message(message, addr)
        print(f"Leader {self.my_address} sent AppendEntries RPC to {addr} with {len(entries)} entries.")

    def update_commit_index(self):
        """Marks log entries as committed if stored on a majority of servers and at least one from the current term."""
        for index in range(len(self.log) - 1, self.commit_index, -1):  # Iterate backward from last entry to commit_index
            match_count = sum(1 for addr in self.match_index if self.match_index[addr] >= index)

            # If a majority of servers have this entry and it's from the current term, commit it
            if match_count > len(self.server_addresses) // 2 and self.log[index].term == self.current_term:
                self.commit_index = index
                self.apply_committed_entries()
                print(f"Leader {self.my_address} committed log entries up to index {self.commit_index}")
                return

    def replicate_log(self):
        """Leader sends AppendEntries RPC to a follower starting from next_index[addr]."""
        for server in self.server_addresses:
            self.send_append_entries(server)

        # Wait for acknowledgments and retry if log inconsistency is detected
        start_time = time.time()
        acks_received = 1  # Leader counts itself

        while time.time() - start_time < 3:  # Wait 3 seconds for responses
            try:
                data, addr = self.socket.recvfrom(4096)
                response = json.loads(data.decode('utf-8'))

                if response.get("msg_type") == "APPEND_ACK":
                    if response.get("success"):
                        acks_received += 1
                        self.match_index[addr] = self.next_index[addr] - 1
                        self.next_index[addr] = len(self.log)  # Move next_index forward

                        # If a majority has replicated, update commit index
                        if acks_received > len(self.server_addresses) // 2:
                            self.update_commit_index()
                            # Send heartbeats to notify followers about committed index
                            self.send_heartbeats()
                            return
                    else:
                        print(f"Log inconsistency detected with {addr}, decrementing next_index and retrying...")
                        self.next_index[addr] = max(0, self.next_index[addr] - 1)  # Move next_index back and retry
                        self.replicate_log(addr)
                        return

            except socket.timeout:
                break  # Timeout, no majority reached

    def apply_committed_entries(self):
        """Apply committed log entries to the state machine."""
        print(f"{self.my_address} committing entries up to index {self.commit_index}")

        # Apply commands from the log that have not been applied to state machine yet
        for i in range(self.last_applied + 1, self.commit_index + 1):
            transaction = self.log[i].transaction # Get transaction from log
            sender, receiver, amount = transaction.sender, transaction.receiver, transaction.amount

            # Update balances in data store
            self.data_store.setdefault(sender, 0)
            self.data_store.setdefault(receiver, 0)
            self.data_store[sender] -= amount
            self.data_store[receiver] += amount
            print(f"{self.my_address} executed transaction: {sender} sent ${amount} to {receiver}")

            # Leader notifies client
            if self.role == "leader":
                response = ClientResponse(success=True, sender=sender, receiver=receiver, amount=amount)
                self.send_message(response, self.client_addr)
            
        # Unlock sender and receiver
        self.locks[sender] = False
        self.locks[receiver] = False

        self.last_applied = self.commit_index  # Update last applied index

        print(f"{self.my_address} Account Balances: {self.data_store}")


    def listen(self):
        # Listen for incoming UDP messages
        print(f"Listening on {self.my_address[0]}:{self.my_address[1]}")
        while self.running:
            try:
                # set timeout to periodically check running flag
                self.socket.settimeout(1)
                data, addr = self.socket.recvfrom(2048) # receive message
                message_data = json.loads(data.decode('utf-8')) # decode message

                msg_type = message_data.get("msg_type")
                print(f"\nReceived {msg_type} from {SERVER_NAMES[addr]}")

                if msg_type == "APPEND_ENTRIES":
                    self.handle_append_entries(message_data, addr)
                elif msg_type == "REQUEST_VOTE":
                    self.handle_vote_request(message_data, addr)
                elif msg_type == "CLIENT_REQUEST" and self.role == "leader":
                    self.handle_client_request(message_data, addr)  # Process client request

            except socket.timeout:
                # If no leader heartbeat is received, start an election
                if self.role == "follower" and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
            except Exception as e:
                print(f"Error receiving data: {e}")
                break
    
    def broadcast_message(self, message, type=""):
        # Broadcast message to all other servers
        # serialize message
        serialized_message = json.dumps(message).encode('utf-8')
        # iterate over all server addresses and send the message
        for server in self.server_addresses:
            try:
                self.socket.sendto(serialized_message, server)  # send the message via UDP
                print(f"Broadcasted {type} message to {SERVER_NAMES[server]}")
            except Exception as e:
                print(f"Error broadcasting to {server}: {e}")
    
    def send_message(self, message, receiver):
        # Send message to specific server
        # serialize message
        serialized_message = json.dumps(message).encode('utf-8') 
        # receiver_addr = DEFAULT_SERVERS[receiver - 1]
        receiver_addr = receiver
        try:
            self.socket.sendto(serialized_message, receiver_addr)  # send the message via UDP
            print(f"Sent message to {SERVER_NAMES[receiver_addr]}: {message}")
        except Exception as e:
            print(f"Error sending message to {SERVER_NAMES[receiver_addr]}: {e}")

    def get_user_input(self):
        while self.running:
            # prompt user for input
            message = input("Enter amount to transfer (type 'exit' to quit): ")
            # check if user wants to exit
            if message.lower() == "exit":
                print("Exiting...")
                self.running = False
                break
            
            # validate input is an int
            if message.isdigit():
                message = int(message)
                receiver = input("Enter receiver (1, 2, or 3): ")
                
                # validate receiver input
                if receiver.isdigit() and 1 <= int(receiver) <= 3:
                    receiver = int(receiver)
                    receiver_addr = DEFAULT_SERVERS[receiver - 1]
                    self.send_message(message, receiver_addr)
                else:
                    print("Invalid receiver. Please enter 1, 2, or 3.")
            else:
                print("Invalid amount. Please enter a valid integer.")

    def run(self):
        # Start listening thread
        threading.Thread(target=self.listen, daemon=True).start()
        # get user input & handle
        self.get_user_input()

        self.socket.close()
        print("Socket closed.")

def main():
    # read client’s port as arg (run on local host IP) from CLI
    if len(sys.argv) < 2:
        print("Usage: python3 bank.py <my_port>")
        print("Example: python3 bank.py 5000")
        sys.exit(1)

    my_ip = "127.0.0.1"
    my_port = int(sys.argv[1])

    # Exclude this server's address from the list of servers
    server_addresses = [addr for addr in DEFAULT_SERVERS if addr != (my_ip, my_port)]

    # Create and run server instance
    server = Server(my_ip, my_port, server_addresses, 1)
    server.run()

if __name__ == "__main__":
    main()