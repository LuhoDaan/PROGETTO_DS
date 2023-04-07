#codice per fare partire i nodi e il coordinatore
from Node import Node

class NodeF:
    def __init__(self, host, port):
        self.host = host
        self.port = port

PORTE = 55000
HOST = "100.101.71.67"

NUMBER_NODES= int(input('Hello User! \n How many Nodes would you like to have on your machine?\n'))
nodes=[]
for i in range(NUMBER_NODES):
    nodes.append(Node(HOST,PORTE+i))
OLD_NODES= NUMBER_NODES
NUMBER_NODES= int(input('Hello User! \n How many Nodes would you like to have on the other machine?\n'))
HOST = input('Insert the IP of the other machine please: \n')
#PORTE = int(input("What's the starting available port?"))
for i in range(NUMBER_NODES):
    NodeF(HOST,PORTE+i)
    nodes.append(NodeF(HOST,PORTE+i))

TOTAL_NODES = NUMBER_NODES+OLD_NODES

#PARTE CLIENT
print(f'It is suggested that for read and write quorum you choose {int(TOTAL_NODES/2 +1)} but you can pick what you want :)')
r_quorum = 0
w_quorum = 0
while not ((0<r_quorum<=TOTAL_NODES) and (0<w_quorum<=TOTAL_NODES)):
    r_quorum = int(input('What is the read quorum? '))
    w_quorum = int(input('What is the write quorum? '))

list_of_nodes = [(r_quorum,w_quorum)]

list_of_nodes+=[(node.host,node.port) for node in nodes]

#export to a json file called nodes the nodes
import json
with open('nodes.json', 'w') as f:
    json.dump(list_of_nodes, f)
    
