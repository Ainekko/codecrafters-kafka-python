import socket  # noqa: F401
import struct  # noqa: F401
import threading

from dataclasses import dataclass  # noqa: F401

@dataclass
class RequestHeader:
    message_size: int
    api_key: int
    api_version: int
    correlation_id: int

def parse_header(message:bytes) -> RequestHeader:
    if len(message) < 12:
        raise ValueError('Data is not complete')
    message_size = struct.unpack(">I", message[0:4])[0]
    api_key = struct.unpack(">h", message[4:6])[0]
    api_version = struct.unpack(">h", message[6:8])[0]
    correlation_id = struct.unpack(">I", message[8:12])[0]
    return RequestHeader(message_size, api_key, api_version, correlation_id)

@dataclass
class ResponseHeader:
    message_size: bytes
    correlation_id: bytes
    error_code: bytes
    api_keys_array_length: bytes
    api_key: bytes
    min_version: bytes
    max_version: bytes
    TAG_BUFFER: bytes
    throttle_time_ms: bytes
    TAG_BUFFER: bytes


@dataclass
class apiKeysversion:
    api_key:int = 18
    min_version:int = 0
    max_version:int = 4
    tag_buffer:int = 0

@dataclass
class DescribeTopicPartitions:
    api_key:int = 75
    min_version:int = 0
    max_version:int = 0
    tag_buffer:int = 0

def parse_response(req, apiKeys: list[apiKeysversion]) -> ResponseHeader:
    api_keys_array_length = struct.pack(">b", len(apiKeys) + 1)  # +1 for the DescribeTopicPartitions entry
    print(f"API keys array length: {len(apiKeys)}")
    api_key_entries = b""
    for apiKey in apiKeys:
        api_key_entry = struct.pack(">hhhb", apiKey.api_key, apiKey.min_version, apiKey.max_version, apiKey.tag_buffer)
        api_key_entries += api_key_entry
   
    throttle_time_ms = struct.pack(">i", 0)
    api_keys_array = api_keys_array_length + api_key_entries
    return api_keys_array, throttle_time_ms



def main():
    # You can use print statements as follows for debugging,
    # they'll be visible when running tests.
    print("Logs from your program will appear here!")


    supported_versions = (0, 1, 2, 3, 4)
    def handle_client(client, addr):
                with client:
                    while True:    
                        message = client.recv(1024)  # wait for client to send data
                        if message == b'':
                            break  # client closed connection
                        header = parse_header(message)
    
                        print(f"Received message with API key {header.message_size} {header.api_key}, version {header.api_version}, correlation ID {header.correlation_id}")
                        if header.api_version not in supported_versions:
                            print(f"Unsupported version {header.api_version}, sending error response")
                            error_code = 35  # Unsupported version
                            apiKeys = apiKeysversion(api_key=header.api_key, min_version=0, max_version=4, tag_buffer=0)
                            api_keys_array, throttle_time_ms = parse_response(header, [apiKeys, DescribeTopicPartitions()])
                            res =  struct.pack(">I", header.correlation_id) + struct.pack(">H", error_code) + api_keys_array + throttle_time_ms + struct.pack(">b", 0)  # TAG_BUFFER
                            message_size = struct.pack(">I", len(res))
                            response = message_size + res
                            client.sendall(response)
                            continue
                        else:
                            error_code = 0  # No error
                            apiKeys = apiKeysversion(api_key=header.api_key, min_version=0, max_version=4, tag_buffer=0)
                            api_keys_array, throttle_time_ms = parse_response(header, [apiKeys, DescribeTopicPartitions()])
                            res =  struct.pack(">I", header.correlation_id) + struct.pack(">H", error_code) + api_keys_array + throttle_time_ms + struct.pack(">b", 0)  # TAG_BUFFER
                            message_size = struct.pack(">I", len(res))
                            print(f"Sending response with message size {len(res)} and correlation ID {header.correlation_id}")
                            response = message_size + res
                            client.sendall(response)

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True: 
        client, addr = server.accept()

        thread = threading.Thread(target=handle_client, args=(client, addr))
        thread.start()

        
    
           


if __name__ == "__main__":
    main()
