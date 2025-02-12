import socket
import threading
import sys
from collections import deque
import json
import hashlib
import heapq
import time
import random
from messages import RequestVote, VoteResponse, AppendEntries, ClientRequest


class LogEntry:
    """Class representing a log entry."""
    def __init__(self, term, command):
        self.term = term
        self.command = command


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
    def __init__(self, my_ip, my_port, server_addresses):
        self.my_address = (my_ip, my_port) # initialize server with address
        self.server_addresses = server_addresses # list of other peer's addresses
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM) # udp socket
        self.socket.bind(self.my_address) # bind to UDP socket
        self.running = True  # flag to control running state of listener thread

        # RAFT variables
        self.current_term = 0
        self.voted_for = None
        self.log = []  # Transaction log
        self.commit_index = 0
        self.role = "follower"  # Initial state is follower
        self.election_timeout = random.uniform(3, 6)  # Randomized timeout for leader election: [T, 2T]
        self.last_heartbeat = time.time()  # Track leader's last heartbeat


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
        self.broadcast_message(message)

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
                
                # Count votes
                if response.get("msg_type") == "VOTE_RESPONSE" and response.get("term") == self.current_term:
                    votes_received += 1
                
                # Become leader if majority votes received
                if votes_received > len(self.server_addresses) // 2: # Majority
                    self.role = "leader"
                    print(f"{SERVER_NAMES[self.my_address]} is now the leader for term {self.current_term}")
                    self.send_heartbeats()
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
            message = AppendEntries(term=self.current_term, leader_id=self.my_address, prev_log_index=len(self.log)-1, prev_log_term=self.log[-1].term if self.log else None, entries=[], leader_commit=self.commit_index).to_dict()
            self.broadcast_message(message)
            time.sleep(1) # heartbeat interval

    def handle_append_entries(self, message, addr):
        """Handle incoming AppendEntries RPC: heartbeats & log replication."""
        term = message.get("term")
        leader_id = message.get("leader_id")
        prev_log_index = message.get("prev_log_index")
        prev_log_term = message.get("prev_log_term")

        if term >= self.current_term:
            # Step down if term is higher
            self.role = "follower"
            self.current_term = term
            self.voted_for = None # Reset vote
            self.last_heartbeat = time.time() # Reset election timeout
            if self.log[prev_log_index].term != prev_log_term:
                print(f"Log mismatch at index {prev_log_index}, rejecting AppendEntries from {leader_id}")
                return
            # Append new entries  FIXME: NEED TO DELETE CONFLICTING ENTRIES
            for entry in message.get("entries", []):
                append_entry = LogEntry(term=entry["term"], command=entry["command"])
                self.log.append(append_entry)
            self.commit_index = message.get("leader_commit", 0)
            # FIXME: ADVANCE STATE MACHINE WITH NEWLY COMMITTED ENTRIES
            print(f"Received heartbeat from leader {leader_id} for term {term}, resetting election timeout.")
        else:
            print(f"Received outdated RPC from leader {leader_id} for term (term {term} < {self.current_term}), ignoring.")

                
    def handle_vote_request(self, message, addr):
        """Handle incoming RequestVote RPC."""
        term = message.get("term")
        candidate_id = message.get("candidate_id")
        last_log_index = message.get("last_log_index")
        last_log_term = message.get("last_log_term")

        # Step down if term is higher
        if term > self.current_term:
            self.current_term = term
            self.role = "follower"
            self.voted_for = None  # Reset vote
        
        # Reject vote if already voted in this term
        if self.voted_for is not None and self.voted_for != candidate_id:
            print(f"Already voted for {self.voted_for} in term {self.current_term}, rejecting {candidate_id}")
            return
        
        # Reject if candidate's log is less complete
        my_last_log_index = len(self.log) - 1
        my_last_log_term = self.log[-1].term if self.log else 0
        if (last_log_term < my_last_log_term) or (last_log_term == my_last_log_term and last_log_index < my_last_log_index):
            print(f"Rejecting vote request from {candidate_id} (outdated log).")
            return

        # Grant vote
        if self.voted_for is None:
            self.voted_for = candidate_id
            response = VoteResponse(term=self.current_term, vote_granted=True).to_dict()
            self.send_message(response, addr)
            print(f"Voted for {candidate_id} in term {term}")

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
                if msg_type == "APPEND_ENTRIES":
                    self.handle_append_entries(message_data, addr)
                elif msg_type == "REQUEST_VOTE":
                    self.handle_vote_request(message_data, addr)

                print(f"\nReceived {msg_type} from {SERVER_NAMES[addr]}")

            except socket.timeout:
                # If no leader heartbeat is received, start an election
                if self.role == "follower" and time.time() - self.last_heartbeat > self.election_timeout:
                    self.start_election()
            except Exception as e:
                print(f"Error receiving data: {e}")
                break
    
    def broadcast_message(self, message):
        # Broadcast message to all other servers
        # serialize message
        serialized_message = json.dumps(message).encode('utf-8')
        # iterate over all server addresses and send the message
        for server in self.server_addresses:
            try:
                self.socket.sendto(serialized_message, server)  # send the message via UDP
                print(f"Broadcasted message to {SERVER_NAMES[server]}")
            except Exception as e:
                print(f"Error broadcasting to {server}: {e}")
    
    def send_message(self, message, receiver):
        # Send message to specific server
        # serialize message
        serialized_message = json.dumps(message).encode('utf-8') 
        receiver_addr = DEFAULT_SERVERS[receiver - 1]
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
                    self.send_message(message, receiver)
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
    server = Server(my_ip, my_port, server_addresses)
    server.run()

if __name__ == "__main__":
    main()


# import socket
# import threading
# import sys

# # Predefined ports and IP addresses for the servers
# DEFAULT_SERVERS = [
#     ("127.0.0.1", 5000),  # server 1
#     ("127.0.0.1", 5001),  # server 2
#     ("127.0.0.1", 5002)   # server 3
# ]
# # Create a mapping of addresses to server identifiers
# SERVER_NAMES = {server: f"Server {i+1}" for i, server in enumerate(DEFAULT_SERVERS)}
# class Server:
#     def __init__(self, my_ip, my_port, server_addresses):
#         self.my_address = (my_ip, my_port)
#         self.server_addresses = server_addresses
#         self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
#         self.socket.bind(self.my_address)
#         self.running = True  # Flag to control the listener thread
#     def listen(self):
#         """Listen for incoming messages."""
#         print(f"Listening on {self.my_address[0]}:{self.my_address[1]}")
#         while self.running:
#             try:
#                 self.socket.settimeout(1)  # Set a timeout to periodically check the running flag
#                 data, addr = self.socket.recvfrom(1024)
#                 if addr in SERVER_NAMES:
#                     print(f"Received from {SERVER_NAMES[addr]}: {data.decode()}")
#                 else:
#                     print(f"Received from unknown server {addr}: {data.decode()}")
#             except socket.timeout:
#                 continue  # Ignore timeouts and keep checking for messages
#             except Exception as e:
#                 print(f"Error receiving data: {e}")
#                 break
    
#     def send_message(self, message, receiver):
#         # serialize message
#         serialized_message = json.dumps(message).encode('utf-8') 
#         try:
#             self.socket.sendto(serialized_message, receiver)  # Send the message via UDP
#             print(f"Sent message to {receiver}: {message}")
#         except Exception as e:
#             print(f"Error broadcasting to {receiver}: {e}")

    
#     def broadcast_message(self, message):
#         """Send a message to all other servers."""
#         for server in self.server_addresses:
#             try:
#                 self.socket.sendto(message.encode(), server)
#                 print(f"Sent to {SERVER_NAMES[server]}: {message}")
#             except Exception as e:
#                 print(f"Error sending to {SERVER_NAMES[server]}: {e}")
#     def run(self):
#         # Start the listening thread
#         threading.Thread(target=self.listen, daemon=True).start()
#         # Allow the user to send messages
#         while self.running:
#             message = input("Enter message to send (type 'exit' to quit): ")
#             if message.lower() == "exit":
#                 print("Exiting...")
#                 self.running = False  # Stop the listener thread
#                 break
#             self.broadcast_message(message)
#         self.socket.close()
#         print("Socket closed. Goodbye!")
# def main():
#     if len(sys.argv) < 2:
#         print("Usage: python3 bank.py <my_port>")
#         print("Example: python3 bank.py 5000")
#         sys.exit(1)
#     my_ip = "127.0.0.1"
#     my_port = int(sys.argv[1])
#     # Exclude this server's address from the list of servers
#     server_addresses = [addr for addr in DEFAULT_SERVERS if addr != (my_ip, my_port)]
#     # Create and run the server
#     server = Server(my_ip, my_port, server_addresses)
#     server.run()
# if __name__ == "__main__":
#     main()