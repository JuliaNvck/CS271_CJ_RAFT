# coordinator.py
import threading
import time
import logging

class ClientCoordinator:
    def __init__(self, messenger, server_addresses):
        self.messenger = messenger
        self.server_addresses = server_addresses
        self.transaction_id = 0
        self.pending_transactions = {}  # {tx_id: {"votes": {}, "acks": {}}}

        # Register this coordinator as the message handler
        self.messenger.message_handler = self.handle_message

    def handle_message(self, message, addr):
        """Handle incoming messages."""
        if message["type"] == "vote":
            self.handle_vote(message["tx_id"], addr, message["vote"])
        elif message["type"] == "ack":
            self.handle_ack(message["tx_id"], addr)

    def initiate_transaction(self, data):
        """Phase 1: Send Prepare to all servers."""
        self.transaction_id += 1
        tx_id = self.transaction_id
        self.pending_transactions[tx_id] = {"votes": {}, "acks": {}}
        
        prepare_msg = {"type": "prepare", "tx_id": tx_id, "data": data}
        self.messenger.broadcast_message(prepare_msg)

    def handle_vote(self, tx_id, server_addr, vote):
        """Process votes from servers."""
        if tx_id not in self.pending_transactions:
            return
        
        self.pending_transactions[tx_id]["votes"][server_addr] = vote
        
        # Check if all votes received
        if len(self.pending_transactions[tx_id]["votes"]) == len(self.server_addresses):
            all_yes = all(vote == "yes" for vote in self.pending_transactions[tx_id]["votes"].values())
            decision = "commit" if all_yes else "abort"
            decision_msg = {"type": decision, "tx_id": tx_id}
            self.messenger.broadcast_message(decision_msg)

    def handle_ack(self, tx_id, server_addr):
        """Process acknowledgments after Commit/Abort."""
        self.pending_transactions[tx_id]["acks"][server_addr] = True
        if len(self.pending_transactions[tx_id]["acks"]) == len(self.server_addresses):
            del self.pending_transactions[tx_id]