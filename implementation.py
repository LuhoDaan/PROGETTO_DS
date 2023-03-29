import socket
import threading
import pickle
from enum import Enum
import time

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

class Node:
    def __init__(self, host, port, kvstore):
        self.host = host
        self.port = port
        self.kvstore = kvstore
        self.data = {}
        self.timestamps = {}
        self.start()

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        self.thread = threading.Thread(target=self.accept_connections)
        self.thread.start()

    def accept_connections(self):
        while True:
            conn, addr = self.sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        msg = self.kvstore.receive_message(conn)
        if msg.msg_type == MessageType.PUT_REQUEST:
            self.put(msg.key, msg.value, msg.timestamp)
            response = Message(MessageType.PUT_RESPONSE, msg.key, timestamp=msg.timestamp)
            self.kvstore.send_message(conn, response)
        elif msg.msg_type == MessageType.GET_REQUEST:
            value, timestamp = self.get(msg.key)
            response = Message(MessageType.GET_RESPONSE, msg.key, value=value, timestamp=timestamp)
            self.kvstore.send_message(conn, response)

    def put(self, key, value, timestamp):
        if key not in self.timestamps or timestamp > self.timestamps[key]:
            self.data[key] = value
            self.timestamps[key] = timestamp

    def get(self, key):
        return self.data.get(key, None), self.timestamps.get(key, None)

class KVStore:
    def __init__(self, nodes, read_quorum, write_quorum):
        self.nodes = nodes
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum

    def put(self, key, value):
        timestamp = time.time()
        responses = []
        for node in self.nodes:
            conn = socket.create_connection((node.host, node.port))
            msg = Message(MessageType.PUT_REQUEST, key, value=value, timestamp=timestamp)
            print(msg)
            self.send_message(conn, msg)
            responses.append(self.receive_message(conn))
            conn.close()

        if len(responses) >= self.write_quorum:
            return True
        return False

    def get(self, key):
        responses = []
        for node in self.nodes:
            conn = socket.create_connection((node.host, node.port))
            msg = Message(MessageType.GET_REQUEST, key)
            self.send_message(conn, msg)
            responses.append(self.receive_message(conn))
            conn.close()

        if len(responses) >= self.read_quorum:
            latest_value = None
            latest_timestamp = None
            for response in responses:
                if response.timestamp is not None and (latest_timestamp is None or response.timestamp > latest_timestamp):
                    latest_value = response.value
                    latest_timestamp = response.timestamp

            return latest_value
        return None

    def send_message(self, sock, message):
        # Serialize the message using pickle
        serialized_message = pickle.dumps(message)

        # Split the serialized message into chunks of 4096 bytes
        chunks = [serialized_message[i:i+4096] for i in range(0, len(serialized_message), 4096)]

        # Send each chunk to the socket
        for chunk in chunks:
            sock.sendall(chunk)

    def receive_message(self, sock):
        # Receive chunks of data from the socket
        chunks = []
        while True:
            chunk = sock.recv(4096)
            print(f"Received chunk of size {len(chunk)}")
            if not chunk:
                break
            chunks.append(chunk)

        # Concatenate the chunks into a single byte string
        serialized_message = b"".join(chunks)

        # Deserialize the byte string using pickle
        message = pickle.loads(serialized_message)

        return message


if __name__=="__main__":
    # Example usage:

    # Create the distributed key-value store
    kvstore = KVStore([], read_quorum=2, write_quorum=2)
    print("ckp1")
    # Create nodes
    node1 = Node('localhost', 47000, kvstore)
    node2 = Node('localhost', 47001, kvstore)
    node3 = Node('localhost', 47002, kvstore)
    print("ckp2")
    kvstore.nodes = [node1, node2, node3]
    print("ckp3")
    # Put and get key-value pairs
    kvstore.put('key1', 'value1')
    print(kvstore.get('key1'))