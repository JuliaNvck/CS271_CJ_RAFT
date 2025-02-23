# server.py
import sys
import json
import time
from udp_messenger import UDPMessenger
from shard_manager import ShardManager
from message import Vote, Ack

class Server:
    def __init__(self, id, cluster, messenger, coordinator_addr):
        self.messenger = messenger
        self.id = id
        self.cluster = cluster
        self.coordinator_addr = coordinator_addr
        self.prepared_transactions = {}  # {tx_id: data}

        self.shard_mgr = ShardManager(self.id, self.cluster)
        self.messenger.message_handler = self.handle_message

    def handle_message(self, message, addr):
        if message.msg_type == "PREPARE":
            self.handle_prepare(message.tx_id, message.data)
        elif message.msg_type in ("COMMIT", "ABORT"):
            self.handle_decision(message.tx_id, message.msg_type)

    def handle_prepare(self, tx_id, data):
        """Vote Yes/No during Phase 1."""
        can_commit = self._can_commit(data)  # Your custom logic
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

    # Define server addresses (exclude current port)
    server_addresses = [(s["ip"], s["port"]) for s in config["servers"] if s["port"] != my_port]

    # Initialize UDP messenger
    messenger = UDPMessenger(
        my_ip="127.0.0.1",
        my_port=my_port,
        server_addresses=server_addresses,
        log_level="info"
    )

    # Run server
    server = Server(my_id, my_cluster, messenger, coordinator_addr)
    print(f"Running as Server on port {my_port}, ID {my_id}, Cluster {my_cluster}...")
    while True:
        # Keep the server running
        time.sleep(1)  # Sleep to avoid busy-waiting

if __name__ == "__main__":
    main()
