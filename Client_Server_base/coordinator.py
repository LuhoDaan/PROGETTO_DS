import socket
import threading
from Message import Message, MessageType

class coordinator:
    
    def __init__(self, host, port, nodes):
        self.host = host
        self.port = port
        self.nodes = nodes
        self.start()
        
    def start(self):
        #set a timeout for calling a function commit every 5 seconds
        self.timer = threading.Timer(5, self.commit())
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.bind((self.host, self.port))
        self.sock.listen(5)
        threading.Thread(target=self.accept_connections).start()
    
    def commit(self):
        #1. bloccare il put in tutti i nodi
        
        data = []
        timestamps= []
        
        for node in self.nodes:
            conn = self.connect_to_node(node)
            self.send_block(conn)
            #2. farsi inviare i dati da tutti i nodi
            data.append(self.receive_message(conn)[0])
            timestamps.append(self.receive_message(conn)[1])
            conn.close()
        print("Blocked all nodes")
        
        #3. confronto i dati e decidere cosa fare
        
        ## timestamp più recente, se valori diversi, prendo quello più frequente, se pari (in numero) prendo a caso
        
        #4. inviare il commit a tutti i nodi
        for node in self.nodes:
            node.send(Message("commit"))
        
    def send_block(self, conn):
     # Serialize the message
        data = Message(MessageType.ANTIENTROPY, 0, 0).serialize()
        msg_len = len(data)
        header = msg_len.to_bytes(4, byteorder='big')
        conn.sendall(header + data)