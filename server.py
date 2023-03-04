import socket
import threading #allows us to separate clients so that they don't have to 
                 #wait for each other to communicate with server


HEADER = 16
PORT = 5050
DISCONNECT_MESSAGE = "!DISCONNECT"

SERVER = socket.gethostbyname(socket.gethostname()) #get IP adress of this computer
                                                    #automatically for you (local ip adress for communication 
                                                    # inside LAN 
ADDR = (SERVER,PORT)

FORMAT = 'utf-8'

server = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #this way we make a new socket and we specify
                                                            #its family (category)
                                                            #second parameter mean that we're streaming data through the 
                                                            #socket

server.bind(ADDR) #bound the socket with the address

def handle_client(conn,addr): #handle communication with specific client (one thread)
    print(f"[NEW CONNECTION] {addr} connected")

    connected = True
    while connected:
    
        msg_length = conn.recv(HEADER).decode(FORMAT) #blocking line of code, it says how many bytes are we receiving from client
        if msg_length:
            msg_length = int(msg_length)
            msg = conn.recv(msg_length).decode(FORMAT)
            print(f"[{addr}] {msg}")
            #manage how to disconnect to notify server
            if msg == DISCONNECT_MESSAGE:
                connected = False

    conn.close()



def start(): #distribute new connections
    print(f"[LISTENING] server is lisenting on {SERVER}")
    server.listen() #listen for new connections
    while True:
        conn, addr = server.accept() #whent the server accepts a connection it will store
                                    # the ip adress of that client and then store an actual object (conn)
                                    #socket object to communicate back
        thread = threading.Thread(target=handle_client,args=(conn,addr)) #ricorda come facevi in acso con posix !
        thread.start()
        print(f"[ACTIVE CONNECTIONS]{threading.activeCount() -1}")#amount of clients connected


print("[STARTING] server is starting")
start()
                                                        
