from transaction import Transaction

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
            'transaction': self.transaction.to_dict() if self.transaction is not None else None,  # Serialize the Transaction object
            'is_2PC': self.is_2PC,
            'tx_id': self.tx_id,
            'committed_2PC': self.committed_2PC
        }
    
    @classmethod
    def from_dict(cls, data):
        """Create a LogEntry from a dictionary."""
        # Deserialize the Transaction object
        transaction = Transaction.from_dict(data['transaction'])
        return cls(
            term=data['term'],
            transaction=transaction,
            is_2PC=data['is_2PC'],
            tx_id=data.get('tx_id', False),  # Default to False if 'tx_id' is missing
            committed_2PC=data.get('committed_2PC', False)  # Default to False if 'committed_2PC' is missing
        )