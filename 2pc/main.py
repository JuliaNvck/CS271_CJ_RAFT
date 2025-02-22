# main.py
import sys
import json
import time
from udp_messenger import UDPMessenger
from message import ClientRequest
from client import Client
from server import Server

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 main.py <my_port>")
        sys.exit(1)

    # Load configuration
    with open("config.json", "r") as f:
        config = json.load(f)
    
    coordinator_config = config["coordinator"]
    coordinator_addr = (coordinator_config["ip"], coordinator_config["port"])
    my_port = int(sys.argv[1])
    for s in config["servers"]:
        if s["port"] == my_port:
            my_id = s["id"]
            my_cluster = s["cluster"]

    # Define server addresses (exclude current port)
    server_addresses = [(s["ip"], s["port"]) for s in config["servers"] if s["port"] != my_port]

    # Initialize UDP messenger
    messenger = UDPMessenger(
        my_ip="127.0.0.1",
        my_port=my_port,
        server_addresses=server_addresses,
        log_level="info"
    )

    # Determine role
    if my_port == coordinator_config["port"]:
        # Run as Client (Coordinator)
        client = Client(messenger, server_addresses)
        print("Running as Client (Coordinator)...")

        transactions = []
        with open('transactions.csv', "r") as file:
            for line in file:
                parts = line.strip().split(",")
                if len(parts) == 3:
                    x, y, amt = parts
                    transactions.append((int(x.strip()), int(y.strip()), int(amt)))
        
        for t in transactions:
            if client.is_intra_shard_transaction(t):
                # issue ClientRequest to leader of correct cluster
                pass
            else:
                # issue 2pc transaction
                # need to first send to only leaders of relevent clusters
                # then send decision (commit/abort) to all servers
                # in relevent clusters
                client.initiate_transaction(t)
            time.sleep(5)

    else:
        # Run as Server
        server = Server(my_id, my_cluster, messenger, coordinator_addr)
        print(f"Running as Server on port {my_port}...")
        while True:
            # Keep the server running
            time.sleep(1)  # Sleep to avoid busy-waiting

if __name__ == "__main__":
    main()