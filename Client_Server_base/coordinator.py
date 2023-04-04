import socket
import threading
from Message import Message, MessageType
from global_commit import global_commit

BUFFER_SIZE = 1024

class coordinator:
    
    def __init__(self, host, port, nodes):
        self.host = host
        self.port = port
        self.nodes = nodes
        self.start()
    
        
    def start(self):
        #set a timeout for calling a function commit every 5 seconds
        
        self.timer = threading.Timer(5, self.commit)
        self.timer.start()
        
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
    
    def commit(self):
        #1. bloccare il put in tutti i nodi
        print("Coordinator: initiating commit")
        data = []
        timestamps= []
        globaldata={}
        
        for node in self.nodes:
            conn = self.connect_to_node(node)
            self.send_block(conn)
            #2. farsi inviare i dati da tutti i nodi
            data.append(self.receive_message(conn)[0])
            timestamps.append(self.receive_message(conn)[1])
            conn.close()
        print("Blocked all nodes")
        
        #3. confronto i dati e decidere cosa fare
        globaldata = global_commit(data,timestamps)
        ## timestamp più recente, se valori diversi, prendo quello più frequente, se pari (in numero) prendo a caso
        
        #4. inviare il commit a tutti i nodi
        for node in self.nodes:
            conn = self.connect_to_node(node)
            data=Message(MessageType.COMMIT, globaldata, 0).serialize()
            msg_len = len(data)
            header = msg_len.to_bytes(4, byteorder='big')
            conn.sendall(header + data)
            conn.close()
            
        self.timer = threading.Timer(5, self.commit())
            
    def send_block(self, conn):
     # Serialize the message
        data = Message(MessageType.ANTIENTROPY, 0, 0).serialize()
        msg_len = len(data)
        header = msg_len.to_bytes(4, byteorder='big')
        conn.sendall(header + data)

    def receive_message(self, conn):
    
    # Receive the length
        header = conn.recv(4)
        if not header:
            return 
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
        #print("Received message:", message.msg_type, message.key, message.value, message.timestamp)
        return message
    
    def connect_to_node(self, node):
        conn = socket.create_connection((node.host, node.port), timeout=10)
        return conn


