# client.py
import sys
import json
import time
from udp_messenger import UDPMessenger
from message import ClientRequest, Prepare, Vote, Commit, Abort, Ack

class Client:
    def __init__(self, messenger, server_addresses, cluster_to_servers):
        self.messenger = messenger
        self.server_addresses = server_addresses
        self.cluster_to_servers = cluster_to_servers
        self.transaction_id = 0
        self.pending_transactions = {}  # {tx_id: {"votes": {}, "acks": {}}}

        self.messenger.message_handler = self.handle_message

    def handle_message(self, message, addr):
        """Handle incoming messages."""
        if message.msg_type == "VOTE":
            self.handle_vote(message.tx_id, addr, message.vote)
        elif message.msg_type == "ACK":
            self.handle_ack(message.tx_id, addr)

    def handle_vote(self, tx_id, server_addr, vote):
        """Process votes from servers."""
        if tx_id not in self.pending_transactions:
            return
        
        self.pending_transactions[tx_id]["votes"][server_addr] = vote
        
        # Check if all votes received
        if len(self.pending_transactions[tx_id]["votes"]) == len(self.server_addresses):
            all_yes = all(vote == "yes" for vote in self.pending_transactions[tx_id]["votes"].values())
            decision = Commit(tx_id=tx_id) if all_yes else Abort(tx_id=tx_id)
            self.messenger.broadcast_message(decision)

    def handle_ack(self, tx_id, server_addr):
        """Process acknowledgments after Commit/Abort."""
        self.pending_transactions[tx_id]["acks"][server_addr] = True
        if len(self.pending_transactions[tx_id]["acks"]) == len(self.server_addresses):
            del self.pending_transactions[tx_id]

    def is_intra_shard_transaction(self, transaction):
        x, y, _ = transaction

        if 0 <= x <= 1000:
            cluster_x = 1
        elif 1001 <= x <= 2000:
            cluster_x = 2
        elif 2001 <= x <= 3000:
            cluster_x = 3
        else:
            raise ValueError(f"Account {x} does not belong to any cluster.")

        if 0 <= y <= 1000:
            cluster_y = 1
        elif 1001 <= y <= 2000:
            cluster_y = 2
        elif 2001 <= y <= 3000:
            cluster_y = 3
        else:
            raise ValueError(f"Account {y} does not belong to any cluster.")

        return cluster_x == cluster_y
    
    def get_clusters(self, transaction):
        x, y, _ = transaction

        if 0 <= x <= 1000:
            cluster_x = 1
        elif 1001 <= x <= 2000:
            cluster_x = 2
        elif 2001 <= x <= 3000:
            cluster_x = 3
        else:
            raise ValueError(f"Account {x} does not belong to any cluster.")

        if 0 <= y <= 1000:
            cluster_y = 1
        elif 1001 <= y <= 2000:
            cluster_y = 2
        elif 2001 <= y <= 3000:
            cluster_y = 3
        else:
            raise ValueError(f"Account {y} does not belong to any cluster.")

        return[cluster_x, cluster_y]
        

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <my_port>")
        sys.exit(1)

    with open("config.json", "r") as f:
        config = json.load(f)
    
    my_port = int(sys.argv[1])

    # Define server addresses
    server_addresses = []
    cluster_to_servers = {}
    
    for server in config["servers"]:
        addr = server["ip"]
        port = server["port"]
        cluster = server["cluster"]
        server_id = server["id"]
        
        if port != my_port:
            server_addresses.append((addr, port))
        
        if cluster not in cluster_to_servers:
            cluster_to_servers[cluster] = []
        cluster_to_servers[cluster].append({"id": server_id, "addr": (addr,port)})

    messenger = UDPMessenger(
        my_ip="127.0.0.1",
        my_port=my_port,
        server_addresses=server_addresses,
        log_level="info"
    )

    # Run client
    client = Client(messenger, server_addresses, cluster_to_servers)
    print(f"Running as Client on port {my_port}...")

    # Load transactions
    transactions = []
    with open('transactions.csv', "r") as file:
        for line in file:
            parts = line.strip().split(",")
            if len(parts) == 3:
                x, y, amt = parts
                transactions.append((int(x.strip()), int(y.strip()), int(amt)))

    # Issue transactions
    time.sleep(5)
    for t in transactions:
        if client.is_intra_shard_transaction(t):
            # issue RAFT transaction
            cluster = client.get_clusters(t)[0]
            receiver = cluster_to_servers[cluster][0] # lowest ID in cluster
            client.messenger.send_message(ClientRequest(t[0], t[1], t[2]), receiver['addr'])
        else:
            # issue 2PC transaction
            client.transaction_id += 1
            tx_id = client.transaction_id
            client.pending_transactions[tx_id] = {"votes": {}, "acks": {}}
            
            for cluster in client.get_clusters(t):
                # lowest ID in each cluster
                receiver = cluster_to_servers[cluster][0]
                client.messenger.send_message(Prepare(tx_id=tx_id, data=t), receiver['addr'])
        time.sleep(10)

if __name__ == "__main__":
    main()