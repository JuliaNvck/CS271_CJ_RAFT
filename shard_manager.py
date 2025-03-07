import os
import csv
import json

from log_entry import LogEntry

class ShardManager:
    '''Manages FileIO for a server's shard data and RAFT log'''
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
        self.data_file = os.path.join(self.shards_folder, f"{self.server_id}_balance.csv")
        
        # Set the RAFT log file path
        self.log_file = os.path.join(self.shards_folder, f"{self.server_id}_log.json")

        # Determine the account offset based on the cluster
        if self.cluster == 1:
            account_offset = 0
        elif self.cluster == 2:
            account_offset = 1001
        elif self.cluster == 3:
            account_offset = 2001
        else:
            raise ValueError("Invalid cluster.")

        # Initialize the CSV file with account balances if it doesn't exist
        if not os.path.exists(self.data_file):
            with open(self.data_file, 'w', newline='') as file:
                writer = csv.writer(file)
                for i in range(1000):
                    account_id = account_offset + i
                    writer.writerow([account_id, 10])  # Initialize balance to 10
        
        # Initialize the RAFT log file if it doesn't exist
        if not os.path.exists(self.log_file):
            self.append_to_log([])  # Create an empty log file

    def updateLogEntryCommittedFlag(self, i, committed_flag: bool):
        log, commit_index = self.get_log()

        if 0 <= i < len(log):
            log[i].committed_2PC = committed_flag
            with open(self.log_file, 'w') as file:
                json.dump({"entries": [entry.to_dict() for entry in log], "commit_index": commit_index}, file, indent=2)
            print(f"Updated log entry {i} committed_2PC to {committed_flag}")
        else:
            print(f"Index {i} is out of bounds for the log with length {len(log)}")
    
    def get_balance(self, account_id):
        if not self.is_account_in_cluster(account_id):
            raise ValueError(f"Account {account_id} does not belong to cluster {self.cluster}.")

        with open(self.data_file, 'r') as file:
            reader = csv.reader(file)
            for row in reader:
                if int(row[0]) == account_id:
                    return int(row[1])
        raise ValueError(f"Account {account_id} not found in the data file.")

    def update_balance(self, account_id, new_balance):
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

    def execute_transaction(self, transaction, commit_index, last_applied_index):
        x, y, amt = transaction

        print(f"Executing transaction: {x} -> {y} : {amt}")

        if not self.is_account_in_cluster(x) and not self.is_account_in_cluster(y):
            raise ValueError(f"Neither account {x} nor account {y} belongs to cluster {self.cluster}.")

        if self.is_account_in_cluster(x):
            x_balance = self.get_balance(x)
            if x_balance < amt:
                raise ValueError(f"Insufficient balance in account {x} balance: {x_balance}.")
            self.update_balance(x, x_balance - amt)

        if self.is_account_in_cluster(y):
            y_balance = self.get_balance(y)
            self.update_balance(y, y_balance + amt)

        # Update the commit index in the log file
        self.store_commit_apply_index(commit_index, last_applied_index)

    def is_account_in_cluster(self, account_id):
        if self.cluster == 1:
            return 0 <= account_id <= 1000
        elif self.cluster == 2:
            return 1001 <= account_id <= 2000
        elif self.cluster == 3:
            return 2001 <= account_id <= 3000
        else:
            return False
    
    # RAFT Log Management Methods
    
    def get_log(self):
        """
        Retrieve the entire RAFT log from disk.
        
        :return: A list of LogEntry objects and the commit index.
        """
        if not os.path.exists(self.log_file):
            return [], -1
        
        with open(self.log_file, 'r') as file:
            log_data = json.load(file)
            commit_index = log_data.get("commit_index", -1)
            log_entries = [LogEntry.from_dict(entry) for entry in log_data.get("entries", [])]
            return log_entries, commit_index
    
    def append_to_log(self, new_entries):
        """
        Append new entries to the RAFT log.
        
        :param new_entries: A list of LogEntry objects to append.
        """
        log, commit_index = self.get_log()
        
        # Convert LogEntry objects to dictionaries
        if isinstance(new_entries, list):
            for entry in new_entries:
                if isinstance(entry, LogEntry):
                    log.append(entry)
        elif isinstance(new_entries, LogEntry):
            log.append(new_entries)
        
        # Save the updated log
        with open(self.log_file, 'w') as file:
            json.dump({"entries": [entry.to_dict() for entry in log], "commit_index": commit_index}, file, indent=2)
    
    def truncate_log(self, last_index):
        """
        Truncate the log to keep entries up to last_index (inclusive).
        Works exactly as list slicing.

        :param last_index: The index of the last entry to keep.
        :return: The truncated log as a list of LogEntry objects.
        """
        log, commit_index = self.get_log()
        
        # Check if last_index is valid
        if last_index < 0 or last_index >= len(log):
            return log  # Return the original log if last_index is invalid
        
        truncated_log = log[:last_index]
        
        with open(self.log_file, 'w') as file:
            json.dump({"entries": [entry.to_dict() for entry in truncated_log], "commit_index": commit_index}, file, indent=2)
        
        return truncated_log
    
    def get_last_log_entry(self):
        log, _ = self.get_log()
        if not log:
            return None
        return log[-1]
    
    def get_log_length(self):
        log, _ = self.get_log()
        return len(log)
    
    def apply_log_entries(self, start_index, end_index=None):
        log, _ = self.get_log()
        if not log:
            return
        
        if end_index is None:
            end_index = len(log) - 1
        
        for i in range(start_index, end_index + 1):
            if 0 <= i < len(log):
                entry = log[i]
                self.execute_transaction(entry.transaction, i)  # Pass the commit index

    def store_commit_apply_index(self, commit_index, last_applied_index):
        log, _ = self.get_log()
        with open(self.log_file, 'w') as file:
            json.dump({"entries": [entry.to_dict() for entry in log], "commit_index": commit_index, "last_applied": last_applied_index}, file, indent=2)

# Example usage
if __name__ == "__main__":
    # Initialize ShardManager for server S1 in cluster 1
    shard_manager = ShardManager(server_id="S1", cluster=1)

    # Create some log entries
    entry1 = LogEntry(term=1, transaction=(50, 100, 5))
    entry2 = LogEntry(term=1, transaction=(60, 200, 3))
    
    # Append entries to the log
    shard_manager.append_to_log([entry1, entry2])
    
    # Get the log length
    log_length = shard_manager.get_log_length()
    print(f"Current log length: {log_length}")
    
    # Apply log entries to update state
    shard_manager.apply_log_entries(0)
    
    # Get updated balances
    balance50 = shard_manager.get_balance(50)
    balance100 = shard_manager.get_balance(100)
    print(f"New balance for account 50: {balance50}")
    print(f"New balance for account 100: {balance100}")