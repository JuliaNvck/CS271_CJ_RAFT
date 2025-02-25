# udp_messenger.py
import socket
import json
import threading
import logging
from message import Message  # Import the Message class

class UDPMessenger:
    def __init__(self, my_ip, my_port, server_config, message_handler=None, log_level="info"):
        self.my_address = (my_ip, my_port)
        self.running = True
        self.socket = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.socket.bind(self.my_address)
        self.message_handler = message_handler  # Callback for handling messages

        self.cluster_to_servers = {}
    
        for server in server_config:
            addr = server["ip"]
            port = server["port"]
            cluster = server["cluster"]
            server_id = server["id"]
            
            # never message oneself by removing from data
            if port != my_port:
                if cluster not in self.cluster_to_servers:
                    self.cluster_to_servers[cluster] = []
                self.cluster_to_servers[cluster].append({"id": server_id, "addr": (addr,port)})

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
                self.logger.info(f"[R] {addr[1]} {message.msg_type}")
                self.logger.debug(f"[R] {addr[1]} {message_dict}")

                # Pass the message to the handler (if provided)
                if self.message_handler:
                    self.message_handler(message, addr)
            except socket.timeout:
                continue

    def clustercast(self, message, cluster):
        """Send a message to all servers (except oneself) in a cluster"""
        if not isinstance(message, Message):
            raise ValueError("Message must be an instance of Message class")
        serialized_message = json.dumps(message.to_dict()).encode('utf-8')

        for server_info in self.cluster_to_servers[cluster]:
            try:
                self.socket.sendto(serialized_message, server_info['addr'])
                self.logger.info(f"[T] {server_info['id']} {message.msg_type}")
                self.logger.debug(f"Clustercast message to {server_info['id']}: {message.to_dict()}")
            except Exception as e:
                self.logger.error(f"Error clustercasting to {server_info['id']}: {e}")

    def send_message(self, message, receiver):
        """Send a message to a specific server."""
        if not isinstance(message, Message):
            raise ValueError("Message must be an instance of Message class")
        serialized_message = json.dumps(message.to_dict()).encode('utf-8')
        try:
            self.socket.sendto(serialized_message, receiver)
            self.logger.info(f"[T] {receiver[1]} {message.msg_type}")
            self.logger.debug(f"[T] {receiver[1]} {message.to_dict()}")
        except Exception as e:
            self.logger.error(f"Error sending message to {receiver}: {e}")

    def stop(self):
        """Stop the listener thread and close the socket."""
        self.running = False
        self.listener_thread.join()  # Wait for the listener thread to finish
        self.socket.close()
        self.logger.info("Socket closed.")
