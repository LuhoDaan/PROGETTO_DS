import socket
import threading
import pickle
import time
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

class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port
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
        msg = self.receive_message(conn)
        if msg.msg_type == MessageType.PUT_REQUEST:
            self.put(msg.key, msg.value, msg.timestamp)
            response = Message(MessageType.PUT_RESPONSE, msg.key)
            self.send_message(conn, response)
        elif msg.msg_type == MessageType.GET_REQUEST:
            value, timestamp = self.get(msg.key)
            response = Message(MessageType.GET_RESPONSE, msg.key, value, timestamp)
            self.send_message(conn, response)
        conn.close()

    def put(self, key, value, timestamp):
        if key not in self.timestamps or timestamp > self.timestamps[key]:
            self.data[key] = value
            self.timestamps[key] = timestamp

    def get(self, key):
        return self.data.get(key, None), self.timestamps.get(key, 0)

    def send_message(self, conn, message):
        data = pickle.dumps(message)
        conn.sendall(data)

    def receive_message(self, conn):
        data = b''
        while True:
            chunk = conn.recv(4096)
            if not chunk:
                break
            data += chunk
        return pickle.loads(data)

class KVStore:
    def __init__(self, nodes, read_quorum, write_quorum):
        self.nodes = nodes
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum

    def put(self, key, value):
        timestamp = int(time.time() * 1000)
        ack_count = 0
        for node in self.nodes:
            try:
                message = Message(MessageType.PUT_REQUEST, key, value, timestamp)
                conn = self.connect_to_node(node)
                self.send_message(conn, message)
                response = self.receive_message(conn)
                if response.msg_type == MessageType.PUT_RESPONSE:
                    ack_count += 1
                conn.close()
            except Exception as e:
                print(f"Error connecting to node {node.host}:{node.port} - {e}")
            if ack_count >= self.write_quorum:
                break

    def get(self, key):
        responses = []
        for node in self.nodes:
            try:
                message = Message(MessageType.GET_REQUEST, key)
                conn = self.connect_to_node(node)
                self.send_message(conn, message)
                response = self.receive_message(conn)
                if response.msg_type == MessageType.GET_RESPONSE:
                    responses.append(response)
                conn.close()
            except Exception as e:
                print(f"Error connecting to node {node.host}:{node.port} - {e}")
            if len(responses) >= self.read_quorum:
                break

        max_timestamp = 0
        max_value = None
        for response in responses:
            if response.timestamp > max_timestamp:
                max_timestamp = response.timestamp
                max_value = response.value

        return max_value

    def connect_to_node(self, node):
        conn = socket.create_connection((node.host, node.port))
        return conn

    def send_message(self, conn, message):
        conn.send_message(conn, message)

    def receive_message(self, conn):
        return conn.receive_message(conn)

# Example usage:

# Create nodes
node1 = Node('localhost', 50000)
node2 = Node('localhost', 50001)
node3 = Node('localhost', 50002)

# Create the distributed key-value store
kvstore = KVStore([node1, node2, node3], read_quorum=2, write_quorum=2)

# Put and get key-value pairs
kvstore.put('key1', 'value1')
print(kvstore.get('key1'))