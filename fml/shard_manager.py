import os
import csv

class ShardManager:
    '''Manages FileIO for a server's shard data'''
    def __init__(self, server_id, cluster):
        """
        Initialize the ShardManager for a specific server and cluster.
        
        :param server_id: The ID of the server (e.g., 'S1', 'S2', etc.).
        :param cluster: The cluster to which the server belongs (e.g., 'C1', 'C2', etc.).
        """
        self.server_id = server_id
        self.cluster = cluster

        # Ensure the 'shards' folder exists
        self.shards_folder = "shards"
        if not os.path.exists(self.shards_folder):
            os.makedirs(self.shards_folder)

        # Set the data file path inside the 'shards' folder
        self.data_file = os.path.join(self.shards_folder, f"{self.server_id}_data.csv")

        # Determine the account offset based on the cluster
        if self.cluster == 1:
            account_offset = 0
        elif self.cluster == 2:
            account_offset = 1001
        elif self.cluster == 3:
            account_offset = 2001
        else:
            raise ValueError("Invalid cluster.")

        # Initialize the CSV file with account balances
        with open(self.data_file, 'w', newline='') as file:
            writer = csv.writer(file)
            for i in range(1000):
                account_id = account_offset + i
                writer.writerow([account_id, 10])  # Initialize balance to 10

    def get_balance(self, account_id):
        """
        Retrieve the balance for a specific account.
        
        :param account_id: The account ID (e.g., 1, 2, ..., 3000).
        :return: The balance as an integer.
        """
        if not self.is_account_in_cluster(account_id):
            raise ValueError(f"Account {account_id} does not belong to cluster {self.cluster}.")

        with open(self.data_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if int(row[0]) == account_id:
                    return int(row[1])
        raise ValueError(f"Account {account_id} not found in the data file.")

    def update_balance(self, account_id, new_balance):
        """
        Update the balance for a specific account.
        
        :param account_id: The account ID (e.g., 1, 2, ..., 3000).
        :param new_balance: The new balance value.
        """
        if not self.is_account_in_cluster(account_id):
            raise ValueError(f"Account {account_id} does not belong to cluster {self.cluster}.")

        rows = []
        account_found = False
        with open(self.data_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if int(row[0]) == account_id:
                    rows.append([row[0], new_balance])
                    account_found = True
                else:
                    rows.append(row)

        if not account_found:
            raise ValueError(f"Account {account_id} not found in the data file.")

        with open(self.data_file, 'w', newline='') as file:
            writer = csv.writer(file)
            writer.writerows(rows)

    def execute_transaction(self, transaction):
        """
        Execute a transaction of the form (x, y, amt).
        
        :param transaction: A tuple (x, y, amt), where:
            - x: The source account ID.
            - y: The target account ID.
            - amt: The amount to transfer.
        """
        x, y, amt = transaction

        if not self.is_account_in_cluster(x) and not self.is_account_in_cluster(y):
            raise ValueError(f"Neither account {x} nor account {y} belongs to cluster {self.cluster}.")

        if self.is_account_in_cluster(x):
            x_balance = self.get_balance(x)
            if x_balance < amt:
                raise ValueError(f"Insufficient balance in account {x}.")
            self.update_balance(x, x_balance - amt)

        if self.is_account_in_cluster(y):
            y_balance = self.get_balance(y)
            self.update_balance(y, y_balance + amt)

    def is_account_in_cluster(self, account_id):
        """
        Check if an account belongs to the current cluster.
        
        :param account_id: The account ID (e.g., 1, 2, ..., 3000).
        :return: True if the account belongs to the cluster, False otherwise.
        """
        if self.cluster == 1:
            return 0 <= account_id <= 1000
        elif self.cluster == 2:
            return 1001 <= account_id <= 2000
        elif self.cluster == 3:
            return 2001 <= account_id <= 3000
        else:
            return False

# Example usage
if __name__ == "__main__":
    # Initialize ShardManager for server S1 in cluster 1
    shard_manager = ShardManager(server_id="S1", cluster=1)

    # Get the balance for account 50
    balance = shard_manager.get_balance(50)
    print(f"Balance for account 50: {balance}")

    # Update the balance for account 50
    shard_manager.update_balance(50, 20)
    print("Updated balance for account 50.")

    # Execute a transaction (50, 100, 5)
    shard_manager.execute_transaction((50, 100, 5))
    print("Executed transaction (50, 100, 5).")