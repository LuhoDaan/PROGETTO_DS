from Coordinator import Coordinator
import json

PORTE = 55000
HOST = "localhost"

with open('nodes.json') as json_file:
    data = json.load(json_file)
    print(data)

class NodeF:
    def __init__(self, host, port):
        self.host = host
        self.port = port

nodes = []

for node in data[1:]:
    nodes.append(NodeF(node[0],node[1]))
    
Coordinator(HOST, PORTE-1, nodes)