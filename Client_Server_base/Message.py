from enum import Enum
import pickle

class MessageType(Enum):
    PUT_REQUEST = 1
    GET_REQUEST = 2

class Message:
    def __init__(self, msg_type, key, value=None, timestamp=None):
        self.msg_type = msg_type
        self.key = key
        self.value = value
        self.timestamp = timestamp
    
    def serialize(self):
        return pickle.dumps(self)
    
    def deserialize(data):
        return pickle.loads(data)