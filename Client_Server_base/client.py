import random
import socket
import time
from Message import Message, MessageType

BUFFER_SIZE = 1024

class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        
class Client:
    def __init__(self, nodes, read_quorum, write_quorum):
        self.nodes = nodes
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum

    def connect_to_node(self, node):
        conn = socket.create_connection((node.host, node.port), timeout=10)
        return conn

    def put(self, key, value):
        
        ack_count = 0
        
        random.shuffle(self.nodes)
        
        for node in self.nodes:
            try:
                message = Message(MessageType.PUT_REQUEST, key, value)
                conn = self.connect_to_node(node)
                self.send_message(conn, message)
                response = self.receive_ack(conn)
                if response == "ACK":
                    ack_count += 1
                if response == "NACK":
                    print("Nodes are not ready to receive any updates, try later")
                    break
                print(ack_count)
                conn.close()
            except Exception as e:
                print(f"Error connecting to node {node.host}:{node.port} - {e}")
            if ack_count >= self.write_quorum:
                print('Success!')
                break

    def get(self, key):
        random.shuffle(self.nodes)
        
        r_quorum = self.nodes[:self.read_quorum]
        for node in r_quorum:
            try:
                message = Message(MessageType.GET_REQUEST, key)
                conn = self.connect_to_node(node)
                self.send_message(conn, message)
                response = self.receive_message(conn)
                conn.close()
            except Exception as e:
                print(f"Error connecting to node {node.host}:{node.port} - {e}")
        
        print(f'The value of {key}, is {response}')
                
        return response

    def send_message(self, conn, message):
        data = message.serialize()
        msg_len = len(data)
        header = msg_len.to_bytes(4, byteorder='big')
        conn.sendall(header + data)

    def receive_ack(self, conn):
        #returns the ACK or NACK
        return conn.recv(4).decode("utf-8")

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
        return message
