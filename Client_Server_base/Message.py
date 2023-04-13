from enum import Enum
import pickle

class MessageType(Enum):
    PUT_REQUEST = 1
    GET_REQUEST = 2
    UPDATE = 3
    COMMIT = 4
    STOP = 5
    PRINT = 6
    ABORT = 7

class Message:
    def __init__(self, msg_type, key=None, value=None, timestamp=None):
        self.msg_type = msg_type
        self.key = key
        self.value = value
        self.timestamp = timestamp

    def serialize(self):
        return pickle.dumps(self)
    
    def deserialize(self,data):
        return pickle.loads(data)