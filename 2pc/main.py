# main.py
import sys
import json
import time
from udp_messenger import UDPMessenger
from coordinator import ClientCoordinator
from server import ParticipantServer

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
        # Run as Coordinator
        coordinator = ClientCoordinator(messenger, server_addresses)
        print("Running as Coordinator...")
        while True:
            data = input("Enter transaction data (or 'exit' to quit): ")
            if data.lower() == "exit":
                break
            coordinator.initiate_transaction(data)
    else:
        # Run as Participant
        participant = ParticipantServer(messenger, coordinator_addr)
        print(f"Running as Server on port {my_port}...")
        while True:
            # Keep the server running
            time.sleep(1)  # Sleep to avoid busy-waiting

if __name__ == "__main__":
    main()