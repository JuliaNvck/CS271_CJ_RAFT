import json
import random
import socket
import sys
from messages import ClientRequest
import time

# Server list (UDP addresses)
SERVERS = [("127.0.0.1", 5000), ("127.0.0.1", 5001), ("127.0.0.1", 5002)]

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

def send_transaction(x, y, amt):
    """Sends a transaction request to a designated server using UDP."""
    # Assume leader is the first server in the list
    server = SERVERS[0]
    # server = random.choice(SERVERS)
    message = ClientRequest(sender=x, receiver=y, amount=amt)
    serialized_message = json.dumps(message.to_dict()).encode('utf-8')

    with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as sock:  # UDP socket
        sock.settimeout(6)  # Timeout for response
        try:
            sock.sendto(serialized_message, server)
            print(f"Sent message to {server}: {message.to_dict()}")

            # Listen for a response from the leader
            response, _ = sock.recvfrom(4096)
            response_data = json.loads(response.decode("utf-8"))
            print(f"Received response: {response_data}")
        except socket.timeout:
            print("Timeout: No response from the server.")
        except Exception as e:
            print(f"Error communicating with server: {e}")

def main():
    if len(sys.argv) != 2:
        print("Usage: python client.py <input_file>")
        sys.exit(1)

    input_file = sys.argv[1]
    transactions = read_input_file(input_file)

    for x, y, amt in transactions:
        send_transaction(x, y, amt)
        time.sleep(1)

if __name__ == "__main__":
    main()
