import json
import socket
import sys
from messages import ClientRequest
import time
import threading

# Server list (UDP addresses)
SERVERS = [("127.0.0.1", 5000), ("127.0.0.1", 5001), ("127.0.0.1", 5002)]

# Define a fixed port for the client to listen on
CLIENT_HOST = "127.0.0.1"
CLIENT_PORT = 6000  # Choose a specific port for listening

# Create a UDP socket for listening and sending transactions
client_socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
client_socket.bind((CLIENT_HOST, CLIENT_PORT))
client_socket.settimeout(6)  # Set a timeout for receiving responses

def read_input_file(file_path):
    """Reads the input file containing (x, y, amt) transactions."""
    transactions = []
    with open(file_path, "r") as file:
        for line in file:
            parts = line.strip().split(",")
            if len(parts) == 3:
                x, y, amt = parts
                transactions.append((x.strip(), y.strip(), int(amt)))
    return transactions

def listen_for_responses():
    """Continuously listens for responses from the leader."""
    print(f"Client listening for responses on {CLIENT_HOST}:{CLIENT_PORT}")
    while True:
        try:
            response, addr = client_socket.recvfrom(4096)  # Listen for response
            response_data = json.loads(response.decode("utf-8"))
            print(f"Received response from {addr}: {response_data}")
        except socket.timeout:
            continue  # Keep listening

def send_transaction(x, y, amt):
    """Sends a transaction request to a designated server using UDP."""
    # Assume leader is the first server in the list
    server = SERVERS[0]
    # server = random.choice(SERVERS)
    message = ClientRequest(sender=x, receiver=y, amount=amt)
    serialized_message = json.dumps(message.to_dict()).encode('utf-8')

    try:
        client_socket.sendto(serialized_message, server)
        print(f"Sent transaction to {server}: {message.to_dict()}")
    except Exception as e:
        print(f"Error communicating with leader: {e}")

    # with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:  # UDP socket
    #     sock.settimeout(6)  # Timeout for response
    #     try:
    #         # Bind the socket to a specific port
    #         sock.bind((CLIENT_HOST, CLIENT_PORT))
    #         sock.sendto(serialized_message, server)
    #         print(f"Sent message to {server}: {message.to_dict()}")

    #         while True:
    #         # Listen for a response from the leader
    #             try: 
    #                 response, _ = sock.recvfrom(4096)
    #                 response_data = json.loads(response.decode("utf-8"))
    #                 print(f"Received response: {response_data}")
    #             except socket.timeout:
    #                 print("Timeout: No response from the server.")
    #                 continue  # Keep waiting for responses
        
    #     except Exception as e:
    #         print(f"Error communicating with server: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    transactions = read_input_file(input_file)

    # Start listening thread for responses
    listener_thread = threading.Thread(target=listen_for_responses, daemon=True)
    listener_thread.start()

    for x, y, amt in transactions:
        send_transaction(x, y, amt)
        time.sleep(0.1)

if __name__ == "__main__":
    main()
