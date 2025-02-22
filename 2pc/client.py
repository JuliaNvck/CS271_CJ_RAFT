# client.py
from message import Message, Prepare, Vote, Commit, Abort, Ack

class Client:
    def __init__(self, messenger, server_addresses):
        self.messenger = messenger
        self.server_addresses = server_addresses
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
        """
        Determine if a transaction is intra-shard (i.e., both accounts belong to the same cluster).
        
        :param transaction: A tuple (x, y, amt), where:
            - x: The source account ID.
            - y: The target account ID.
            - amt: The amount to transfer.
        :return: True if the transaction is intra-shard, False otherwise.
        """
        x, y, _ = transaction

        # Determine the cluster for account x
        if 0 <= x <= 1000:
            cluster_x = 1
        elif 1001 <= x <= 2000:
            cluster_x = 2
        elif 2001 <= x <= 3000:
            cluster_x = 3
        else:
            raise ValueError(f"Account {x} does not belong to any cluster.")

        # Determine the cluster for account y
        if 0 <= y <= 1000:
            cluster_y = 1
        elif 1001 <= y <= 2000:
            cluster_y = 2
        elif 2001 <= y <= 3000:
            cluster_y = 3
        else:
            raise ValueError(f"Account {y} does not belong to any cluster.")

        # Check if both accounts belong to the same cluster
        return cluster_x == cluster_y
        
    def initiate_transaction(self, data):
        """Phase 1: Send Prepare to all servers."""
        self.transaction_id += 1
        tx_id = self.transaction_id
        self.pending_transactions[tx_id] = {"votes": {}, "acks": {}}
        
        prepare_msg = Prepare(tx_id=tx_id, data=data)
        self.messenger.broadcast_message(prepare_msg)