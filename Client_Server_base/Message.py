from enum import Enum

class MessageType(Enum):
    PUT_REQUEST = 1
    PUT_RESPONSE = 2
    GET_REQUEST = 3
    GET_RESPONSE = 4

class Message:
    def __init__(self, msg_type, key, value=None, timestamp=None):
        self.msg_type = msg_type
        self.key = key
        self.value = value
        self.timestamp = timestamp