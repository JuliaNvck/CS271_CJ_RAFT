import sys
import json
import time
from udp_messenger import UDPMessenger
from messages import ClientRequest, Prepare, Vote, Commit, Abort, Ack
import os, csv
import threading

class Client:
    def __init__(self, messenger, cluster_to_servers):
        self.messenger = messenger
        self.cluster_to_servers = cluster_to_servers
        self.transaction_id = 0
        self.pending_transactions = {}  # {tx_id: {"votes": {}, "acks": {}, "transaction": t}}
        self.transaction_times = {}  # Track start and end times for each transaction
        self.completed_transactions = 0  # Count of completed transactions
        self.total_latency = 0  # Cumulative latency for all transactions
        self.start_time = time.time()  # Track when the client started processing transactions

        self.messenger.message_handler = self.handle_message

    def handle_message(self, message, addr):
        """Handle incoming messages."""
        if message.msg_type == "VOTE":
            self.handle_vote(message.tx_id, addr, message.vote)
        elif message.msg_type == "ACK":
            self.handle_ack(message.tx_id, addr)
        elif message.msg_type == "CLIENT_RESPONSE":
            self.handle_client_response(message)

    def handle_vote(self, tx_id, server_addr, vote):
        """Process votes from servers."""
        if tx_id not in self.pending_transactions:
            return
        
        self.pending_transactions[tx_id]["votes"][server_addr] = vote
        
        # Check if all votes received
        if len(self.pending_transactions[tx_id]["votes"]) == 2:
            all_yes = all(vote == "yes" for vote in self.pending_transactions[tx_id]["votes"].values())
            decision = Commit(tx_id=tx_id) if all_yes else Abort(tx_id=tx_id)
            c_x, c_y = self.get_clusters(self.pending_transactions[tx_id]["transaction"])
            self.messenger.clustercast(decision, c_x)
            self.messenger.clustercast(decision, c_y)

    def handle_ack(self, tx_id, server_addr):
        """Process acknowledgments after Commit/Abort."""
        if tx_id not in self.pending_transactions:
            return
            
        self.pending_transactions[tx_id]["acks"][server_addr] = True

        # Track transaction completion time for 2PC transactions
        if tx_id in self.transaction_times:
            end_time = time.time()
            start_time = self.transaction_times[tx_id]
            latency = end_time - start_time
            self.total_latency += latency
            self.completed_transactions += 1
            del self.transaction_times[tx_id]
            
            # Clean up completed transactions
            if len(self.pending_transactions[tx_id]["acks"]) >= 2:
                del self.pending_transactions[tx_id]

    def handle_client_response(self, message):
        """Process ClientResponse messages for non-2PC transactions."""
        # Extract the transaction details from the message
        sender = message.sender
        receiver = message.receiver
        amount = message.amount

        # Find the corresponding transaction in transaction_times
        tx_key = (sender, receiver, amount)
        if tx_key in self.transaction_times:
            end_time = time.time()
            start_time = self.transaction_times[tx_key]
            latency = end_time - start_time
            self.total_latency += latency
            self.completed_transactions += 1
            del self.transaction_times[tx_key]

    def Performance(self):
        """Calculate and display performance metrics."""
        if self.completed_transactions == 0:
            print("No transactions completed yet.")
            return

        current_time = time.time()
        elapsed_time = current_time - self.start_time
        
        # Calculate average latency
        average_latency = self.total_latency / self.completed_transactions
        
        # Calculate throughput based on elapsed time since client started
        throughput = self.completed_transactions / elapsed_time if elapsed_time > 0 else 0

        print("-" * 40)
        print(f"  Completed Transactions: {self.completed_transactions}")
        print(f"  Elapsed Time: {elapsed_time:.2f} seconds")
        print(f"  Throughput: {throughput:.2f} transactions/second")
        print(f"  Average Latency: {average_latency:.2f} seconds")
        print("-" * 40)

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

        return cluster_x, cluster_y
    
    def PrintBalance(self, account_id):
        c = self.get_clusters((account_id, account_id, None))[0]
        balances = {}
        for server in self.cluster_to_servers[c]:
            shard_file = f"shards/{server['id']}_balance.csv"
            if os.path.exists(shard_file):
                with open(shard_file, 'r') as file:
                    reader = csv.reader(file)
                    for row in reader:
                        if int(row[0]) == account_id:
                            balances[server['id']] = int(row[1])

        print("-" * 15)
        print("{:<7} {:<7}".format("Server", "Balance"))
        print("-" * 15)
        for server_id, balance in balances.items():
            print("{:<7} ${:<7}".format(server_id, balance))
        print("-" * 15)

    def PrintDatastore(self):
        print("-" * 40)
        for server_id in range(1, 10):
            log_file = f"shards/{server_id}_log.json"
            if not os.path.exists(log_file):
                print(f"{server_id}: Log file not found.")
                continue

            with open(log_file, "r") as file:
                log_data = json.load(file)

            commit_index = log_data.get("commit_index", -1)
            entries = log_data.get("entries", [])

            committed_transactions = []
            tx_id_to_index = {}  # track 2PC transactions by tx_id

            for i, entry in enumerate(entries):
                if i > commit_index:
                    continue  # skip entries beyond the commit index

                transaction = entry.get("transaction", {})
                is_2pc = entry.get("is_2PC", False)
                tx_id = entry.get("tx_id", None)
                committed_2pc = entry.get("committed_2PC", False)

                if not is_2pc:
                    # non-2PC transaction: committed if index <= commit_index
                    committed_transactions.append((transaction.get("sender", "null"),
                                                transaction.get("receiver", "null"),
                                                transaction.get("amount", "null")))
                else:
                    # 2PC transaction: must have a corresponding entry with committed_2PC = true
                    if tx_id not in tx_id_to_index:
                        tx_id_to_index[tx_id] = i  # track the first occurrence of this tx_id
                    elif committed_2pc:
                        # found the corresponding committed_2PC entry
                        first_index = tx_id_to_index[tx_id]
                        first_entry = entries[first_index]
                        first_transaction = first_entry.get("transaction", {})
                        committed_transactions.append((first_transaction.get("sender", "null"),
                                                    first_transaction.get("receiver", "null"),
                                                    first_transaction.get("amount", "null")))
            
            print(f"{server_id}: ", end='')
            if committed_transactions:
                for tx in committed_transactions:
                    print(f"({tx[0]}, {tx[1]}, {tx[2]}) ", end='')
                print()
            else:
                print()
        print("-" * 40)

def issue_transaction(client, t, cluster_to_servers):
    if t[0] not in range(0, 3001):
        print(f"Account {t[0]} not found. Skipping: {t}")
        return
    if t[1] not in range(0, 3001):
        print(f"Account {t[1]} not found. Skipping: {t}")
        return

    # Record transaction start time
    if client.is_intra_shard_transaction(t):
        # Store using transaction tuple as key for non-2PC transactions
        client.transaction_times[(t[0], t[1], t[2])] = time.time()
        
        # issue RAFT transaction
        cluster = client.get_clusters(t)[0]
        receiver = cluster_to_servers[cluster][0] # lowest ID in cluster
        client.messenger.send_message(ClientRequest(t[0], t[1], t[2]), receiver['addr'])
    else:
        # issue 2PC transaction
        client.transaction_id += 1
        tx_id = client.transaction_id
        
        # Store transaction start time
        client.transaction_times[tx_id] = time.time()
        client.pending_transactions[tx_id] = {"votes": {}, "acks": {}, "transaction": t}

        c_x, c_y = client.get_clusters(t)
        # message someone from x
        recv = cluster_to_servers[c_x][0] # lowest id in cluster
        client.messenger.send_message(Prepare(tx_id=tx_id, data=t), recv['addr'])

        # message someone from y
        recv = cluster_to_servers[c_y][0] # lowest id in cluster
        client.messenger.send_message(Prepare(tx_id=tx_id, data=t), recv['addr'])

def main():
    if len(sys.argv) < 2:
        print("Usage: python3 client.py <my_port>")
        sys.exit(1)

    with open("config.json", "r") as f:
        config = json.load(f)
    
    my_port = int(sys.argv[1])

    cluster_to_servers = {}
    for server in config["servers"]:
        addr = server["ip"]
        port = server["port"]
        cluster = server["cluster"]
        server_id = server["id"]
        
        if cluster not in cluster_to_servers:
            cluster_to_servers[cluster] = []
        cluster_to_servers[cluster].append({"id": server_id, "addr": (addr,port)})

    messenger = UDPMessenger(
        my_ip="127.0.0.1",
        my_port=my_port,
        server_config=config['servers'],
        log_level="info"
    )

    client = Client(messenger, cluster_to_servers)
    print(f"Running as Client on port {my_port}...")

    print(
    "Commands:\n"
    "  - PrintBalance <account#> (pb <account#>)\n"
    "  - PrintDatastore (pd)\n"
    "  - Performance (perf)\n"
)

    transactions = []
    with open('transactions.csv', "r") as file:
        for line in file:
            parts = line.strip().split(",")
            if len(parts) == 3:
                x, y, amt = parts
                transactions.append((int(x.strip()), int(y.strip()), int(amt.strip())))

    tx_index = 0
    while True:
        command = sys.stdin.readline()
        if command == '\n':
            if tx_index < len(transactions):
                issue_transaction(client, transactions[tx_index], cluster_to_servers)
                tx_index += 1
            continue

        command = command.strip()
        if command.startswith("PrintBalance") or command.startswith("pb"):
            parts = command.split()
            if len(parts) == 2:
                try:
                    account_id = int(parts[1])
                    client.PrintBalance(account_id)
                except ValueError:
                    print("Invalid account number.")
        elif command.startswith("PrintDatastore") or command.startswith('pd'):
            client.PrintDatastore()
        elif command.startswith("Performance") or command.startswith('perf'):
            client.Performance()


if __name__ == "__main__":
    main()