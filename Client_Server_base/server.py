import socket
import Message

HOST = "localhost"
PORT = 53000
BUFFER_SIZE = 1024

def serve():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.bind((HOST, PORT))
    sock.listen()
    print("Server listening on {}:{}".format(HOST, PORT))

    conn, addr = sock.accept()
    print("Connected by", addr)

    while True:
        # Receive the length
        header = conn.recv(4)
        if not header:
            break

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
        message = data.decode("utf-8").strip()
        print("Received message:", message)

    conn.close()
    sock.close()

serve()

def server():
    return
