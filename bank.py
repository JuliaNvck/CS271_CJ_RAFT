import socket
import threading
import sys
from collections import deque
import json
import hashlib
import heapq
import time
import random

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

    def listen(self):
        # Listen for incoming UDP messages
        print(f"Listening on {self.my_address[0]}:{self.my_address[1]}")
        while self.running:
            try:
                # set timeout to periodically check running flag
                self.socket.settimeout(1)
                data, addr = self.socket.recvfrom(2048) # receive message
                message_data = json.loads(data.decode('utf-8')) # decode message

                print(f"Received {message_data} from {SERVER_NAMES[addr]}")

            except socket.timeout:
                # don't need to increment clock on message loss or drop since using udp on local host
                continue  # ignore timeouts and keep checking for messages
            except Exception as e:
                # don't need to increment clock on message loss or drop since using udp on local host
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
