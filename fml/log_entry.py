class LogEntry:
    """Class representing a log entry."""
    def __init__(self, term, transaction):
        self.term = term
        self.transaction = transaction
    
    def to_dict(self):
        """Convert LogEntry to a dictionary for serialization."""
        return {
            'term': self.term,
            'transaction': self.transaction
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create a LogEntry from a dictionary."""
        return cls(data['term'], data['transaction'])
