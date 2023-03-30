import socket
import time
import Message
import argparse
import random

# defined command line options
# this also generates --help and error handling
CLI=argparse.ArgumentParser()
CLI.add_argument(
  "--lista",  # name on the CLI - drop the `--` for positional/required parameters
  nargs="*",  # 0 or more values expected => creates a list
  default=[],  # default if nothing is provided
)
#nel client dobbiamo decidere quanti server contattare per far si che possa fare put e get


HOST = "localhost"
PORT = 53000
BUFFER_SIZE = 1024

w_quorum_n = lambda len: len//2 + 1
r_quorum_n = lambda len: len//2 + 1

# method to make the "lista" a list: 
# Namespace(lista=['123.1234.41234.3241', '1234.142.4'])
# to
# Namespace(lista=['123.1234.41234.3241', '1234.142.4'])

def __init__(self, nodes, read_quorum, write_quorum):
        self.nodes = nodes
        self.read_quorum = read_quorum
        self.write_quorum = write_quorum

def put(self, key, value):
    timestamp = int(time.time() * 1000)
    ack_count = 0
    for node in self.nodes:
        try:
            message = Message(Message.MessageType.PUT_REQUEST, key, value, timestamp)
            conn = self.connect_to_node(node)
            self.send_message(conn, message)
            response = self.receive_message(conn)
            if response.msg_type == Message.MessageType.PUT_RESPONSE:
                ack_count += 1
            conn.close()
        except Exception as e:
            print(f"Error connecting to node {node.host}:{node.port} - {e}")
        if ack_count >= self.write_quorum:
            break

def client():

    nodes = CLI.parse_args() #questa parte serve per passargli la lista di ip:porta, però dobbiamo infilarla nell'init e salvare le variabili come self in qualche modo
    if nodes.lista == []:
        print ("errore")
        return
    print(nodes.lista[1])
    #choose a random set of servers from the list
    write_quorum = random.sample(nodes.lista, w_quorum_n(len(nodes.lista)))
    read_quorum = random.sample(nodes.lista, r_quorum_n(len(nodes.lista)))


    """ sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    message = input('Enter a message: ')
    # Create the header
    msg_len = len(message.encode('utf-8'))
    header = msg_len.to_bytes(4, byteorder='big')
    # Send the header and message data
    sock.sendall(header + message.encode('utf-8'))
    sock.close() """

client()