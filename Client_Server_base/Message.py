from enum import Enum
import pickle

class MessageType(Enum):
    PUT_REQUEST = 1
    GET_REQUEST = 2
    ANTIENTROPY = 3
    COMMIT = 4

class Message:
    def __init__(self, msg_type, key, value=None):
        self.msg_type = msg_type
        self.key = key
        self.value = value
        
    
    def serialize(self):
        return pickle.dumps(self)
    
    def deserialize(self,data):
        return pickle.loads(data)