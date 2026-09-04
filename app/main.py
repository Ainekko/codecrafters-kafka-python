import array
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

@dataclass
class DTPResponse:
    throttle_time_ms:bytes
    topics_array_length:bytes
    error_code:bytes
    topic_id:bytes
    is_internal:bytes
    partition_array_length:bytes
    topic_authorized_operations:bytes
    TAG_BUFFER:bytes
    next_cursor:bytes
    tag_buffer:bytes

def apiVersionsParser(req, apiKeys: list[apiKeysversion]) -> ResponseHeader:
    api_keys_array_length = struct.pack(">b", len(apiKeys) + 1)  # +1 for the DescribeTopicPartitions entry
    print(f"API keys array length: {len(apiKeys)}")
    api_key_entries = b""
    for apiKey in apiKeys:
        api_key_entry = struct.pack(">hhhb", apiKey.api_key, apiKey.min_version, apiKey.max_version, apiKey.tag_buffer)
        api_key_entries += api_key_entry
   
    throttle_time_ms = struct.pack(">i", 0)
    api_keys_array = api_keys_array_length + api_key_entries
    return api_keys_array, throttle_time_ms


def parse_header(message:bytes) -> RequestHeader:
    if len(message) < 12:
        raise ValueError('Data is not complete')
    message_size = struct.unpack(">I", message[0:4])[0]
    api_key = struct.unpack(">h", message[4:6])[0]
    api_version = struct.unpack(">h", message[6:8])[0]
    correlation_id = struct.unpack(">I", message[8:12])[0]
    return RequestHeader(message_size, api_key, api_version, correlation_id)

def parse_topics_api(message):
    #we should pass the message size bytes

    client_id_length = struct.unpack('>h', message[12:14])[0]
    body_start = 12 + 2 + client_id_length + 1  # 12 bytes for header, 2 bytes for client_id length, + the bytes for the client id content + 1 for the buffer
    topic_name_size  = struct.unpack(">b", message[body_start+1:body_start + 2])[0] - 1
    topic_name =  message[body_start + 2:body_start + 2 + topic_name_size].decode('utf-8')
    correlation_id = struct.unpack(">I", message[8:12])[0]
    return topic_name, correlation_id
    #also pass the rest of the ehader bytes

    #and then index into the array topic length

    #then here we read the topic

def send_response(client, header: bytes, body: bytes):
    payload = header + body
    message_size = struct.pack(">I", len(payload))
    resp = message_size + payload
    client.sendall(resp)


def DTPHandler(message, client, header):
    topic_name, correlation_id = parse_topics_api(message)
    topic_name = topic_name.encode('utf-8')
    print(f"Received DESCRIBE_TOPIC_PARTITIONS request for topic: {topic_name.decode('utf-8')}")
    # Construct the response
    # DTPResponse() later idk what i am trynna do here

    header = struct.pack(">I", correlation_id) + struct.pack(">b", 0)  # TAG_BUFFER
    body  = struct.pack(">b", len(topic_name)) + topic_name

    send_response(client, header, body)



def apiVersionsHandler(client, header, supported_versions):

    if header.api_version not in supported_versions:
        print(f"Unsupported version {header.api_version}, sending error response")
        error_code = 35  # Unsupported version
        apiKeys = apiKeysversion(api_key=header.api_key, min_version=0, max_version=4, tag_buffer=0,)
        api_keys_array, throttle_time_ms = apiVersionsParser(header, [apiKeys, DescribeTopicPartitions()], )
        res_header =  struct.pack(">I", header.correlation_id)
        body = struct.pack(">H", error_code) + api_keys_array + throttle_time_ms + struct.pack(">b", 0)  # TAG_BUFFER
        send_response(client, res_header, body)
                                        
    else:
        error_code = 0  # No error
        apiKeys = apiKeysversion(api_key=header.api_key, min_version=0, max_version=4, tag_buffer=0)
        api_keys_array, throttle_time_ms = apiVersionsParser(header, [apiKeys, DescribeTopicPartitions()])
        res_header =  struct.pack(">I", header.correlation_id)
        body = struct.pack(">H", error_code) + api_keys_array + throttle_time_ms + struct.pack(">b", 0)  # TAG_BUFFER
        send_response(client, res_header, body)



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

                        if header.api_key == 18:  # API_VERSIONS
                            apiVersionsHandler(client, header, supported_versions)   
                        elif header.api_key == 75:  # DESCRIBE_TOPIC_PARTITIONS
                            topic_name = parse_topics_api(message)
                            print(f"Parsed topic name: {topic_name}")
                            DTPHandler(message, client, header)
                        else:
                            print(f"Unknown API key: {header.api_key}, sending error response")
                            error_code = 35  # Unsupported version
                            apiKeys = apiKeysversion(api_key=header.api_key, min_version=0, max_version=4, tag_buffer=0)
                            api_keys_array, throttle_time_ms = apiVersionsParser(header, [apiKeys, DescribeTopicPartitions()])
                            res_header =  struct.pack(">I", header.correlation_id)
                            body = struct.pack(">H", error_code) + api_keys_array + throttle_time_ms + struct.pack(">b", 0)  # TAG_BUFFER               

    server = socket.create_server(("localhost", 9092), reuse_port=True)
    
    while True: 
        client, addr = server.accept()

        thread = threading.Thread(target=handle_client, args=(client, addr))
        thread.start()

        
    
           


if __name__ == "__main__":
    main()
