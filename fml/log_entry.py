class LogEntry:
    """Class representing a log entry."""
    def __init__(self, term, transaction, is_2PC, tx_id=False, committed_2PC=False):
        self.term = term
        self.transaction = transaction
        self.is_2PC = is_2PC
        self.tx_id = tx_id
        self.committed_2PC = committed_2PC
    
    def to_dict(self):
        """Convert LogEntry to a dictionary for serialization."""
        return {
            'term': self.term,
            'transaction': self.transaction,
            'is_2PC': self.is_2PC
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create a LogEntry from a dictionary."""
        return cls(data['term'], data['transaction'])
