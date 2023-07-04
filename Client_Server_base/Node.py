import pickle
import socket
import threading
from Message import MessageType, Message

BUFFER_SIZE = 1024

class Node:
    def __init__(self, host, port):
        self.host = host
        self.port = port
        self.data = {}
        self.timestamps = {}
        self.start()
        self.blocked = False

    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            self.sock.bind((self.host, self.port))
        except:
            print(f"The port number {self.port} is already in use, please choose another one")
            return
        self.sock.listen(5)
        threading.Thread(target=self.accept_connections).start()

    def accept_connections(self):
        while True:
            conn, addr = self.sock.accept()
            threading.Thread(target=self.handle_client, args=(conn, addr)).start()

    def handle_client(self, conn, addr):
        
        msg = self.receive_message(conn)

        if msg.msg_type == MessageType.PUT_REQUEST:
            if self.blocked:
                self.send_timestamp(conn, 0)
            else:
                self.blocked = True
                self.send_timestamp(conn, msg.key)
                
        elif msg.msg_type == MessageType.COMMIT:
            self.data[msg.key] = msg.value
            self.timestamps[msg.key] = msg.timestamp
            self.blocked = False
        
        elif msg.msg_type == MessageType.ABORT:
            self.blocked = False


        elif msg.msg_type == MessageType.GET_REQUEST:
            value, timestamp = self.get(msg.key)
            response = Message(MessageType.GET_REQUEST, value=value, timestamp=timestamp)
            self.send_message(conn, response)
            
        elif msg.msg_type == MessageType.UPDATE:
            self.data[msg.key] = msg.value
            self.timestamps[msg.key] = msg.timestamp


        elif msg.msg_type == MessageType.PRINT:
            self.print_data()
            
            
        elif msg.msg_type == MessageType.STOP:
            print(f"Stopping node: {self.host} : {self.port}")
            self.sock.close()
            return
          
        conn.close()

    def get(self, key):
        if key not in self.data:                    
            return 'null', 0
        return self.data[key], self.timestamps[key]

    def send_timestamp(self, conn, key):
        if key not in self.timestamps:
            conn.sendall((1).to_bytes(4, byteorder='big'))
        else:
            conn.sendall((self.timestamps[key]+1).to_bytes(4, byteorder='big'))

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
        
        print(f"Printing data from node: {self.host} : {self.port}")
        print("{:<10} {:<10} {:<20}".format("Key", "Value", "Timestamp"))
        for key in self.data:
            print("{:<10} {:<10} {:<20}".format(key, self.data[key], self.timestamps[key]))