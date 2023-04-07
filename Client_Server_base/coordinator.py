import socket
from threading import Timer
from Message import Message, MessageType
from global_commit import global_commit

BUFFER_SIZE = 1024

class RepeatTimer(Timer):  
    def run(self):  
        while not self.finished.wait(self.interval):  
            self.function(*self.args,**self.kwargs)  
            print(' ')

class Coordinator:
    
    def __init__(self, host, port, nodes):
        self.host = host
        self.port = port
        self.nodes = nodes
        self.start()
    
        
    def start(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        timer = RepeatTimer(20,self.commit)  
        timer.start()
        print('Threading started')

    def commit(self):
        
        data = []
        timestamps= []
        globaldata={}
        
        print("Coordinator: initiating commit")
        
        for node in self.nodes:
            conn = self.connect_to_node(node)
            #1. bloccare il put in tutti i nodi
            self.send_block(conn)
            
            #2. farsi inviare i dati da tutti i nodi

            mylist = self.receive_message(conn)
            data.append(mylist[0])
            timestamps.append(mylist[1])
            conn.close()
        
        print("Correctly blocked all nodes")
        
        #3. confrontare i dati e decidere cosa fare
        globaldata = global_commit(data,timestamps)
        
        #4. inviare il commit a tutti i nodi
        for node in self.nodes:
            conn = self.connect_to_node(node)
            data=Message(MessageType.COMMIT, globaldata, 0).serialize()
            msg_len = len(data)
            header = msg_len.to_bytes(4, byteorder='big')
            conn.sendall(header + data)
            conn.close()
        
        print("Correctly committed all nodes")
        print(globaldata)
            
    def send_block(self, conn):
        
        data = Message(MessageType.ANTIENTROPY, 0, 0).serialize()
        msg_len = len(data)
        header = msg_len.to_bytes(4, byteorder='big')
        conn.sendall(header + data)

    def receive_message(self, conn):
    
        header = conn.recv(4)
        if not header:
            print("Qualcuno ha mandato un messaggio con header vuoto")
            return 

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
    
    def connect_to_node(self, node):
        conn = socket.create_connection((node.host, node.port), timeout=10)
        return conn