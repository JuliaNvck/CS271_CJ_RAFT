# server.py
from message import Message, Prepare, Vote, Commit, Abort, Ack

class Server:
    def __init__(self, messenger, coordinator_addr):
        self.messenger = messenger
        self.coordinator_addr = coordinator_addr
        self.prepared_transactions = {}  # {tx_id: data}

        # Register this server as the message handler
        self.messenger.message_handler = self.handle_message

    def handle_message(self, message, addr):
        """Handle incoming messages."""
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