import socket  # noqa: F401
import struct  # noqa: F401



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")

    # TODO: Uncomment the code below to pass the first stage
    #
    server = socket.create_server(("localhost", 9092), reuse_port=True)
    server.accept() # wait for client
    while True:
        client, addr = server.accept()
        data = {
           struct.pack(">I", 1) + struct.pack(">I", 7)
        }
        client.sendall(data)


if __name__ == "__main__":
    main()
