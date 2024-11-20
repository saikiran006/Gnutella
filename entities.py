# message.py
import json
from typing import List

class Message:
    def __init__(self, message_id: str, message_type: str, message: str, ttl: int, port: int, file_server_ports: List[int] = None, file_size: int = 0):
        if file_server_ports is None:
            file_server_ports = []
        self.message_id = message_id
        self.message_type = message_type
        self.message = message
        self.ttl = ttl
        self.port = port
        self.file_server_ports = file_server_ports
        self.file_size = file_size

    def to_json(self):
        """ Convert the message object to JSON string. """
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(json_str):
        """ Create a message object from a JSON string. """
        data = json.loads(json_str)
        return Message(**data)

    def __repr__(self):
        return (f"Message(message_id={self.message_id}, message_type={self.message_type}, "
                f"message={self.message}, ttl={self.ttl}, port={self.port}, "
                f"file_server_ports={self.file_server_ports}, file_size={self.file_size})")


class Peer:
    def __init__(self, peer_port, speed, reader=None, writer=None):
        self.peer_port = peer_port
        self.speed = speed
        self.reader = reader
        self.writer = writer

    def __repr__(self):
        return f"Peer(port={self.peer_port}, speed={self.speed})"
