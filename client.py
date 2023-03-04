import socket


HEADER = 16
PORT = 5050
DISCONNECT_MESSAGE = "!DISCONNECT"

SERVER = socket.gethostbyname(socket.gethostname()) #get IP adress of this computer
                                                    #automatically for you (local ip adress for communication 
ADDR = (SERVER,PORT)

FORMAT = 'utf-8'

client = socket.socket(socket.AF_INET,socket.SOCK_STREAM) #this way we make a new socket and we specify
                                                            #its family (category)
                                                            #second parameter mean that we're streaming data through the 
                                                            #socket

client.connect(ADDR) # it's the corresponding to bind socket for servers

def send(msg):
    message = msg.encode(FORMAT) #encode string into a bytelike object