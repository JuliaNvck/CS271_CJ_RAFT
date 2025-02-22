# test_2pc.py
import unittest
from unittest.mock import MagicMock
from coordinator import ClientCoordinator
from server import ParticipantServer

class MockUDPMessenger:
    def __init__(self):
        self.broadcast_messages = []
        self.sent_messages = []

    def broadcast_message(self, message):
        self.broadcast_messages.append(message)

    def send_message(self, message, receiver):
        self.sent_messages.append((message, receiver))

class TestClientCoordinator(unittest.TestCase):
    def setUp(self):
        self.messenger = MockUDPMessenger()
        self.server_addresses = [("127.0.0.1", 5001), ("127.0.0.1", 5002)]
        self.coordinator = ClientCoordinator(self.messenger, self.server_addresses)

    def test_initiate_transaction(self):
        """Test that the coordinator sends a Prepare message."""
        self.coordinator.initiate_transaction("test_data")
        self.assertEqual(len(self.messenger.broadcast_messages), 1)
        self.assertEqual(self.messenger.broadcast_messages[0]["type"], "prepare")
        self.assertEqual(self.messenger.broadcast_messages[0]["data"], "test_data")

    def test_handle_vote_commit(self):
        """Test that the coordinator commits when all servers vote Yes."""
        self.coordinator.initiate_transaction("test_data")
        self.coordinator.handle_vote(1, ("127.0.0.1", 5001), "yes")
        self.coordinator.handle_vote(1, ("127.0.0.1", 5002), "yes")
        self.assertEqual(len(self.messenger.broadcast_messages), 2)
        self.assertEqual(self.messenger.broadcast_messages[1]["type"], "commit")

    def test_handle_vote_abort(self):
        """Test that the coordinator aborts when any server votes No."""
        self.coordinator.initiate_transaction("test_data")
        self.coordinator.handle_vote(1, ("127.0.0.1", 5001), "yes")
        self.coordinator.handle_vote(1, ("127.0.0.1", 5002), "no")
        self.assertEqual(len(self.messenger.broadcast_messages), 2)
        self.assertEqual(self.messenger.broadcast_messages[1]["type"], "abort")

class TestParticipantServer(unittest.TestCase):
    def setUp(self):
        self.messenger = MockUDPMessenger()
        self.coordinator_addr = ("127.0.0.1", 5000)
        self.server = ParticipantServer(self.messenger, self.coordinator_addr)

    def test_handle_prepare_yes(self):
        """Test that the server votes Yes when it can commit."""
        self.server.handle_prepare(1, "test_data")
        self.assertEqual(len(self.messenger.sent_messages), 1)
        self.assertEqual(self.messenger.sent_messages[0][0]["type"], "vote")
        self.assertEqual(self.messenger.sent_messages[0][0]["vote"], "yes")

    def test_handle_decision_commit(self):
        """Test that the server commits on receiving a Commit decision."""
        self.server.handle_prepare(1, "test_data")
        self.server.handle_decision(1, "commit")
        self.assertEqual(len(self.messenger.sent_messages), 2)
        self.assertEqual(self.messenger.sent_messages[1][0]["type"], "ack")
        self.assertNotIn(1, self.server.prepared_transactions)  # Data is committed

    def test_handle_decision_abort(self):
        """Test that the server aborts on receiving an Abort decision."""
        self.server.handle_prepare(1, "test_data")
        self.server.handle_decision(1, "abort")
        self.assertEqual(len(self.messenger.sent_messages), 2)
        self.assertEqual(self.messenger.sent_messages[1][0]["type"], "ack")
        self.assertNotIn(1, self.server.prepared_transactions)  # Data is aborted

if __name__ == "__main__":
    unittest.main()