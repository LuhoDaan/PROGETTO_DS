from client import Client
import json

#import from a json file called nodes the nodes
with open('nodes.json') as json_file:
    data = json.load(json_file)
    print(data)

class NodeF:
    def __init__(self, host, port):
        self.host = host
        self.port = port

nodes = []

r_quorum, w_quorum = data[0]

for node in data[1:]:
    #print(node)
    nodes.append(NodeF(node[0],node[1]))

client = Client(nodes,r_quorum,w_quorum)

while True:
    command = input("Enter a command (PUT, GET, or STOP): ").strip(' ').upper()

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
        if value[0] != "null":
            print(f"The value for key '{key}' is '{value}'.")
        else:
            print(f"No value found for key '{key}'.")

    elif command == "STOP":
        print("Stopping everything.")
        client.stop()
        break

    #TODO: print the nodes
    elif command == "PRINT":
        client.print()

    else:
        print("Invalid command. Please enter PUT, GET, or STOP.")