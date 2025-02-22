# udp_messenger.py
import socket
import json
import threading
import logging
from message import Message  # Import the Message class

class UDPMessenger:
    def __init__(self, my_ip, my_port, server_addresses, message_handler=None, log_level="info"):
        self.my_address = (my_ip, my_port)
        self.server_addresses = server_addresses
        self.running = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.my_address)
        self.message_handler = message_handler  # Callback for handling messages

        # Configure logging
        self.logger = logging.getLogger("UDPMessenger")
        self.logger.setLevel(logging.DEBUG)  # Set base level to DEBUG

        # Create console handler
        ch = logging.StreamHandler()
        if log_level.lower() == "info":
            ch.setLevel(logging.INFO)
        elif log_level.lower() == "debug":
            ch.setLevel(logging.DEBUG)
        else:
            raise ValueError("log_level must be 'info' or 'debug'")

        # Add handler to logger
        self.logger.addHandler(ch)

        # Start the listener thread
        self.listener_thread = threading.Thread(target=self.listen, daemon=True)
        self.listener_thread.start()

    def listen(self):
        """Listen for incoming UDP messages and pass them to the message handler."""
        self.logger.info(f"Listening on {self.my_address[0]}:{self.my_address[1]}")
        while self.running:
            try:
                self.socket.settimeout(1)  # Set timeout to periodically check running flag
                data, addr = self.socket.recvfrom(2048)  # Receive message
                message_dict = json.loads(data.decode('utf-8'))  # Decode message

                # Convert the dictionary to a Message object
                message = Message.from_dict(message_dict)

                # Log the received message
                self.logger.info(f"[RX] {addr[1]} {message.msg_type}")
                self.logger.debug(f"[RX] {addr[1]} {message_dict}")

                # Pass the message to the handler (if provided)
                if self.message_handler:
                    self.message_handler(message, addr)
            except socket.timeout:
                continue

    def broadcast_message(self, message):
        """Broadcast a message to all servers."""
        if not isinstance(message, Message):
            raise ValueError("Message must be an instance of Message class")
        serialized_message = json.dumps(message.to_dict()).encode('utf-8')
        for server in self.server_addresses:
            try:
                self.socket.sendto(serialized_message, server)
                self.logger.info(f"[TX] {server[1]} {message.msg_type}")
                self.logger.debug(f"Broadcasted message to {server}: {message.to_dict()}")
            except Exception as e:
                self.logger.error(f"Error broadcasting to {server}: {e}")

    def send_message(self, message, receiver):
        """Send a message to a specific server."""
        if not isinstance(message, Message):
            raise ValueError("Message must be an instance of Message class")
        serialized_message = json.dumps(message.to_dict()).encode('utf-8')
        try:
            self.socket.sendto(serialized_message, receiver)
            self.logger.info(f"[TX] {receiver[1]} {message.msg_type}")
            self.logger.debug(f"[TX] {receiver[1]} {message.to_dict()}")
        except Exception as e:
            self.logger.error(f"Error sending message to {receiver}: {e}")

    def stop(self):
        """Stop the listener thread and close the socket."""
        self.running = False
        self.listener_thread.join()  # Wait for the listener thread to finish
        self.socket.close()
        self.logger.info("Socket closed.")
