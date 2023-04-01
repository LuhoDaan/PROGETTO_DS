from Node import Node
from client import Client
from getIp import get_local_ip

#PARTE SERVER

class NodeF:
    def __init__(self, host, port):
        self.host = host
        self.port = port

PORTE = 55500
BUFFER_SIZE = 1024
HOST = get_local_ip()
print(f'Your IP is {HOST}')
NUMBER_NODES= int(input('Hello User! \n How many Nodes would you like to have on your machine?\n'))
nodes=[]
for i in range(NUMBER_NODES):
    nodes.append(Node(HOST,PORTE+i))
OLD_NODES= NUMBER_NODES
NUMBER_NODES= int(input('Hello User! \n How many Nodes would you like to have on the other machine?\n'))
HOST = input('Insert the IP of the other machine please: \n')
PORTE = int(input("What's the starting available port?"))
for i in range(NUMBER_NODES):
    NodeF(HOST,PORTE+i)
    nodes.append(NodeF(HOST,PORTE+i))


TOTAL_NODES = NUMBER_NODES+OLD_NODES

#PARTE CLIENT
print(f'It is suggested that for read and write quorum you choose{TOTAL_NODES/2 +1} but you can pick what you want :)')
r_quorum = 0
w_quorum = 0
while not ((0<r_quorum<=TOTAL_NODES) and (0<w_quorum<=TOTAL_NODES)):
    r_quorum = int(input('What is the read quorum?')) 
    w_quorum = int(input('What is the write quorum?')) 
client = Client(nodes,r_quorum,w_quorum)
while True:
    command = input("Enter a command (PUT, GET, or STOP): ").upper()

    if command == "PUT":
        key = input("Enter a key: ")
        value = input("Enter a value: ")
        #funzione del client
        client.put(key,value)
        print(f"Added key '{key}' with value '{value}' to data.")

    elif command == "GET":
        key = input("Enter a key: ")
        #funzione del client
        value = client.get(key)
        if value != "null":
            print(f"The value for key '{key}' is '{value}'.")
        else:
            print(f"No value found for key '{key}'.")

    elif command == "STOP":
        print("Stopping interaction.")
        break

    elif command == "PRINT":
        for node in nodes[:OLD_NODES]:
            node.print_data()

    else:
        print("Invalid command. Please enter PUT, GET, or STOP.")
    