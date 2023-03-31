import pickle
import socket
import threading
import Message
from Message import MessageType, Message


BUFFER_SIZE = 1024

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
        threading.Thread(target=self.accept_connections).start()

    def accept_connections(self):
        while True:
            conn, addr = self.sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()
            #come esce il while?


    def handle_client(self, conn, addr):
        msg = self.receive_message(conn)
        if msg.msg_type == MessageType.PUT_REQUEST:
            self.put(msg.key, msg.value, msg.timestamp)
            print("Received PUT request:", msg.key, msg.value, msg.timestamp)
            self.send_ack(conn)
        elif msg.msg_type == MessageType.GET_REQUEST:
            value, timestamp = self.get(msg.key)
            response = Message(MessageType.GET_REQUEST, msg.key, value, timestamp)
            self.send_message(conn, response)
        conn.close()

    def put(self, key, value, timestamp):
        if key not in self.timestamps or timestamp > self.timestamps[key]:
            self.data[key] = value
            self.timestamps[key] = timestamp

    def get(self, key):
        if key not in self.data:
            return 'null', 0
        return self.data.get(key, None), self.timestamps.get(key, 0)

    def send_ack(self, conn):
        conn.sendall("ACK".encode('utf-8'))

    def send_message(self, conn, message):
     # Serialize the message
        data = message.serialize()
        msg_len = len(data)
        header = msg_len.to_bytes(4, byteorder='big')
        conn.sendall(header + data)

    def receive_message(self, conn):
        
        # Receive the length
        header = conn.recv(4)
        if not header:
            raise RuntimeError("ERROR")

        # Parse the header
        msg_len = int.from_bytes(header[0:4], byteorder="big")

        # Receive the message data
        chunks = []
        bytes_recd = 0
        while bytes_recd < msg_len:
            chunk = conn.recv(min(msg_len - bytes_recd,
                                BUFFER_SIZE))
            if not chunk:
                raise RuntimeError("ERROR")
            chunks.append(chunk)
            bytes_recd += len(chunk)

        data = b"".join(chunks)

        # Print the message
        message = Message.deserialize(self,data)
        print("Received message:", message.msg_type, message.key, message.value, message.timestamp)
        return message
    
    def print_data(self):
        print("Data:")
        print("{:<10} {:<10} {:<20}".format("Key", "Value", "Timestamp"))
        for key in self.data:
            value, timestamp = self.get(key)
            print("{:<10} {:<10} {:<20}".format(key, value, timestamp))
