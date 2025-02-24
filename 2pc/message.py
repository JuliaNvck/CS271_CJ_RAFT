# message.py
class Message:
    """Base class for all message types."""
    def __init__(self, msg_type):
        self.msg_type = msg_type

    def to_dict(self):
        """Converts message to dictionary format."""
        return self.__dict__

    @staticmethod
    def from_dict(message_dict):
        """Converts a dictionary to a Message object."""
        msg_type = message_dict["msg_type"]
        message_dict.pop('msg_type')
        if msg_type == "CLIENT_REQUEST":
            return ClientRequest(**message_dict)
        elif msg_type == "APPEND_ENTRIES":
            return AppendEntries(**message_dict)
        elif msg_type == "REQUEST_VOTE":
            return RequestVote(**message_dict)
        elif msg_type == "VOTE_RESPONSE":
            return VoteResponse(**message_dict)
        elif msg_type == "APPEND_ACK":
            return AppendAck(**message_dict)
        elif msg_type == "CLIENT_RESPONSE":
            return ClientResponse(**message_dict)
        elif msg_type == "PREPARE":
            return Prepare(**message_dict)
        elif msg_type == "VOTE":
            return Vote(**message_dict)
        elif msg_type == "COMMIT":
            return Commit(**message_dict)
        elif msg_type == "ABORT":
            return Abort(**message_dict)
        elif msg_type == "ACK":
            return Ack(**message_dict)
        else:
            raise ValueError(f"Unknown message type: {msg_type}")

# RAFT-related message types
class ClientRequest(Message):
    """Message for client transaction requests."""
    def __init__(self, sender, receiver, amount):
        super().__init__("CLIENT_REQUEST")
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

class AppendEntries(Message):
    """RAFT AppendEntries RPC."""
    def __init__(self, term, leader_id, prev_log_index, prev_log_term, entries, leader_commit):
        super().__init__("APPEND_ENTRIES")
        self.term = term
        self.leader_id = leader_id
        self.prev_log_index = prev_log_index
        self.prev_log_term = prev_log_term
        self.entries = entries  # List of log entries
        self.leader_commit = leader_commit

class RequestVote(Message):
    """RAFT RequestVote RPC."""
    def __init__(self, term, candidate_id, last_log_index, last_log_term):
        super().__init__("REQUEST_VOTE")
        self.term = term
        self.candidate_id = candidate_id
        self.last_log_index = last_log_index
        self.last_log_term = last_log_term

class VoteResponse(Message):
    """Response to a RequestVote RPC."""
    def __init__(self, term, vote_granted):
        super().__init__("VOTE_RESPONSE")
        self.term = term
        self.vote_granted = vote_granted

class AppendAck(Message):
    """Response to an AppendEntries RPC."""
    def __init__(self, success):
        super().__init__("APPEND_ACK")
        self.success = success

class ClientResponse(Message):
    """Response to a client request."""
    def __init__(self, success, sender, receiver, amount):
        super().__init__("CLIENT_RESPONSE")
        self.success = success
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

# 2PC-related message types
class Prepare(Message):
    """Prepare message for 2PC."""
    def __init__(self, tx_id, data):
        super().__init__("PREPARE")
        self.tx_id = tx_id
        self.data = data

class Vote(Message):
    """Vote message for 2PC."""
    def __init__(self, tx_id, vote):
        super().__init__("VOTE")
        self.tx_id = tx_id
        self.vote = vote

class Commit(Message):
    """Commit message for 2PC."""
    def __init__(self, tx_id):
        super().__init__("COMMIT")
        self.tx_id = tx_id

class Abort(Message):
    """Abort message for 2PC."""
    def __init__(self, tx_id):
        super().__init__("ABORT")
        self.tx_id = tx_id

class Ack(Message):
    """Acknowledgment message for 2PC."""
    def __init__(self, tx_id):
        super().__init__("ACK")
        self.tx_id = tx_id