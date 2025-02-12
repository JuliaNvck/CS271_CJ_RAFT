class Message:
    """Base class for all message types."""
    def __init__(self, msg_type):
        self.msg_type = msg_type

    def to_dict(self):
        """Converts message to dictionary format."""
        return self.__dict__

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
