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
        self.globaldata = {}
        self.timestamps = {}
        self.start()
        self.blocked = False


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
        
        if msg.msg_type == MessageType.ANTIENTROPY:
            self.blocked=True
            self.send_message(conn, [self.data, self.timestamps])   #manda i dizionari
            
        elif msg.msg_type == MessageType.COMMIT:
            ##TODO: commit
            self.globaldata = msg.key
            self.data = msg.key
            self.blocked=False
            
        elif msg.msg_type == MessageType.PUT_REQUEST:
            if self.blocked:
                self.send_nack(conn)
                return
            self.put(msg.key, msg.value)
            self.send_ack(conn)
            
        elif msg.msg_type == MessageType.GET_REQUEST:
            value = self.get(msg.key)
            response = Message(MessageType.GET_REQUEST, msg.key, value)
            self.send_message(conn, response)
        
        conn.close()

    def put(self, key, value):
        if key not in self.timestamps:
            self.timestamps[key] = 0
        else:
            self.timestamps[key] +=1 
        self.data[key] = value
        
    def get(self, key):
        if key not in self.globaldata:
            return 'null', 0
        return self.globaldata.get(key, None)

    def send_ack(self, conn):
        conn.sendall("ACK".encode('utf-8'))
        
    def send_nack(self, conn):
        conn.sendall("NACK".encode('utf-8'))

    def send_message(self, conn, message):
        # Serialize the message
        data = pickle.dumps(message)
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

        message = Message.deserialize(self,data)
        return message
    
    def print_data(self):
        print("Data:")
        print("{:<10} {:<10} {:<20}".format("Key", "Value", "Timestamp"))
        for key in self.data:
            value = self.data.get(key)
            timestamp = self.timestamps.get(key)
            print("{:<10} {:<10} {:<20}".format(key, value, timestamp))
            
