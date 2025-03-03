class Transaction:
    """Class representing a transaction."""
    def __init__(self, sender, receiver, amount):
        self.sender = sender
        self.receiver = receiver
        self.amount = amount

    def to_dict(self):
        """Convert the Transaction object to a dictionary."""
        return {
            'sender': self.sender,
            'receiver': self.receiver,
            'amount': self.amount
        }

    @classmethod
    def from_dict(cls, data):
        """Create a Transaction object from a dictionary."""
        return cls(data['sender'], data['receiver'], data['amount'])