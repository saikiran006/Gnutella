import socket
import sys
import os
import asyncio
import random
import json
from dataclasses import dataclass, field

import sys
from logger import Logger
from file_server import File_Server



MIN_CONNS = 3
MAX_CONNS = 10
HOST="127.0.0.1"

@dataclass
class Message:
    message_id: str
    message_type: str
    message: str
    ttl: int
    port: int
    file_server_port:int = field(default=0) 

    def to_json(self):
        """ Convert the message object to JSON string. """
        return json.dumps(self.__dict__)

    @staticmethod
    def from_json(json_str):
        """ Create a message object from a JSON string. """
        data = json.loads(json_str)
        return Message(**data)

class Peer:
    def __init__(self, peer_port, speed, reader=None, writer=None):
        self.peer_port=peer_port
        self.speed = speed
        self.reader = reader
        self.writer = writer

    def __repr__(self):
        return f"Peer(port={self.peer_port}, speed={self.speed})"

class GnutellaPeer:
    def __init__(self, port, speed,directory):
        self.port = port
        self.speed = speed
        self.directory=directory
        self.peers = {}  # Store peers in a dictionary
        self.messages = {}  # Store messages for each peer
        self.requests={} # Store requests coming from other peers 
        self.response_futures = {} # Store responses to my requests 
        self.files=[]
        self.message_counter = 0
        self.set_connection_limits()
        self.read_files_in_directory()
        print(self.files)
        self.query_msg_ids=[]
        log_file_path = self.directory+"/output.log"  # Specify your log file path
        self.logger = Logger(log_file_path)

        self.file_server=File_Server(directory=directory)

        # Redirect standard output to the logger
        # sys.stdout = self.logger
        # logger.write("logging started")

    def read_files_in_directory(self):
        for filename in os.listdir(self.directory):
        # Create full path
            full_path = os.path.join(self.directory, filename)
            
            # Check if it is a file
            if os.path.isfile(full_path):
                self.files.append(filename)
    
    async def start_file_server(self):
        await self.file_server.start_server()
        print("File Server Started on",self.file_server.port)

    def set_connection_limits(self):
        """ Set min and max connections based on speed. """
        global MIN_CONNS, MAX_CONNS
        if self.speed < 50:
            MIN_CONNS = 2
            MAX_CONNS = 4
        elif 50 <= self.speed < 100:
            MIN_CONNS = 2
            MAX_CONNS = 6
        else:
            MIN_CONNS = 2
            MAX_CONNS = 10

    def get_message_id(self):
        """ Generate a unique message ID using the peer's port and current message count. """
        self.message_counter += 1  # Increment the counter for each new message
        return f"{self.port}-{self.message_counter}"  # Combine port and counter to create a message ID

    def print_connected_peers(self):
        """ Print currently connected peers. """
        print("Currently connected peers:")
        for peer in self.peers.values():
            print(peer)

    async def start_server(self):
        server = await asyncio.start_server(self.handle_connection, '127.0.0.1', self.port)
        print(f"Serving on {server.sockets[0].getsockname()}")
        async with server:
            await server.serve_forever()

    async def connect_to_initial_peers(self, initial_peers):
        """ Connect to a random selection of initial peers based on connection limits. """
        # connections_needed = random.randint(MIN_CONNS, MAX_CONNS)
        # selected_peers = random.sample(initial_peers, min(connections_needed, len(initial_peers)))

        # for host, port in selected_peers:
        #     await self.connect_to_peer(host, port)
        if initial_peers:
            # Get the last peer from the list
            last_peer = initial_peers[-1]
            port = last_peer
            
            # Connect only to the last peer
            await self.connect_to_peer(port)
            await self.send_message(port, "SYN", "Hello, Peer!", ttl=5)
    
    async def connect_to_peer(self, port):
        """Connects to a peer and handles communication."""
        try:
            reader, writer = await asyncio.open_connection(HOST, port)
            print(f"Connected to peer at {HOST}:{port}")

            # Store the peer in the dictionary
            self.peers[port] = Peer(port, self.speed, reader, writer)
            self.messages[port] = []  # Initialize message list for the peer
            self.response_futures[port] = []

            # Start communication loop
            asyncio.create_task(self.communicate_with_peer(port))

        except ConnectionRefusedError:
            print(f"Failed to connect to {port}")
            return None, None  # Return None if connection fails
        except asyncio.CancelledError:
            print(f"Connection to {port} cancelled")
            return None, None  # Return None on cancellation
        except Exception as e:
            print(f"Error during communication with {port}: {e}")
            return None, None  # Return None on other errors
    
    async def communicate_with_peer(self, port):
        """ Send a message to a specific peer and wait for a response. """
        peer = self.peers.get(port)

        if not peer:
            print(f"Peer {port} not found.")
            return

        try:
            while True:
                # Check for outgoing messages to send
                if port in self.messages and self.messages[port]:
                    msg_obj = self.messages[port].pop(0)  # Get and remove the first message
                    if peer.writer:
                        serialized_msg = msg_obj.to_json().encode()
                        peer.writer.write(serialized_msg + b'\n')
                        await peer.writer.drain()
                        print(f"Sent message to {port}: {msg_obj}")

                        if msg_obj.message_type == "ACK":
                            continue

                        # Wait for a response from the peer
                        data = await peer.reader.read(4096)  # Read from the peer
                        if data:
                            json_message = data.decode().strip()
                            try:
                                received_msg = Message.from_json(json_message)
                                print(f"Received from {port}: {received_msg}")

                                # Notify the response future
                                if port in self.response_futures and self.response_futures[port]:
                                    response_future = self.response_futures[port].pop(0)
                                    if not response_future.done():
                                        response_future.set_result(received_msg)

                            except json.JSONDecodeError:
                                print(f"Invalid message format from {port}")
                    else:
                        print(f"Peer {port} writer not available.")

                await asyncio.sleep(1)

        except Exception as e:
            print(f"Error in communication with peer {port}: {e}")
    
    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        port = addr[1]  # Extract the port number from the address
        print(f"Connected to port: {port}")  # Logging the port number only
        # self.peers[port] = Peer(port, self.speed, reader, writer)  # Store using port number

        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break  # Connection closed by the peer

                json_message = data.decode().strip()
                print(f"Received raw data from port {port}: {json_message}")  # Log raw data received
                response_message = None
                try:
                    received_msg = Message.from_json(json_message)
                    # print(f"Received from port {port}: {received_msg}")
                    if received_msg.message_type == "SYN":
                        await self.connect_to_peer(received_msg.port)
                        msg_obj = Message(message_id=self.get_message_id(),message_type="ACK", message="Connected Successfully", ttl=1, port=self.port)
                        response_message = msg_obj.to_json().encode()
                    elif received_msg.message_type == "Query":
                        response_message = await self.handle_query(received_msg)
                    else:
                        print(f"Unknown message type: {received_msg.message_type}")  # Log unknown message types
                except json.JSONDecodeError:
                    print(f"Invalid message format from port {port}: {json_message}")  # More detailed error logging
                except Exception as e:
                    print(f"An error occurred: {e}")                    

                if response_message:
                    writer.write(response_message + b'\n')  # Ensure response_message is encoded properly
                    await writer.drain()

        except asyncio.CancelledError:
            print(f"Connection to port {port} closed")
        except Exception as e:
            print(f"Exception in handle_connection {e}")
        finally:
            writer.close()
            await writer.wait_closed()
            del self.peers[port]  # Remove the peer from the dictionary using the port
        
    async def handle_query(self,received_msg):
        print(f"In handle_query: {received_msg}")
        msg_id=received_msg.message_id
        file_name = received_msg.message
        TTL = received_msg.ttl
        if(msg_id in self.query_msg_ids):
            msg_obj = Message(message_id=self.get_message_id(), message_type="QueryFAIL", message=f"File '{file_name}' not found in the network", ttl=1, port=self.port)
            serialized_msg = msg_obj.to_json().encode()
            return serialized_msg
        
        self.query_msg_ids.append(msg_id)
        # Check if the file is present in the local directory
        if file_name in self.files:
            print("File Found")
            msg_obj = Message(message_id=self.get_message_id(),message_type="QueryHIT", message=f"File '{file_name}' found at port {self.file_server.port}", ttl=1, port=self.port, file_server_port=self.file_server.port)
            serialized_msg = msg_obj.to_json().encode()
            return serialized_msg
        else:
            print(f"File '{file_name}' not found locally. Checking TTL: {TTL}")
            if TTL > 1:
                # Decrease TTL and forward the query to connected peers
                new_ttl = TTL - 1
                # msg_obj = Message(message_type="Query", message=file_name, ttl=new_ttl, port=received_msg.port)
                for peer_address in self.peers.keys():
                    print(f"Forwarded query for '{file_name}' to peers with TTL={new_ttl}")
                    response = await self.send_message(peer_address, "Query", file_name, new_ttl,message_id=msg_id)
                    if(response.message_type=="QueryHIT"):
                        return response.to_json().encode()
                msg_obj = Message(message_id=self.get_message_id(), message_type="QueryFAIL", message=f"File '{file_name}' not found in the network", ttl=1, port=self.port)
                serialized_msg = msg_obj.to_json().encode()
                return serialized_msg
                # No immediate response sent when forwarding the message
            else:
                # TTL is 0, respond with QUERY_FAIL
                print(f"TTL for file '{file_name}' reached 0. Returning QUERY_FAIL.")
                msg_obj = Message(message_id=self.get_message_id(),message_type="QueryFAIL", message=f"File '{file_name}' not found in the network", ttl=1, port=self.port)
                serialized_msg = msg_obj.to_json().encode()
                return serialized_msg


    async def send_message(self, port, message_type, message, ttl, message_id=None):
        """ Add a structured message to the queue for a specific peer. """
        if message_id is None:
            message_id = self.get_message_id()
        msg_obj = Message(message_id=message_id, message_type=message_type, message=message, ttl=ttl, port=self.port)
        
        if port not in self.messages:
            self.messages[port] = []
        self.messages[port].append(msg_obj)
        print(f"Added message for {port}: {msg_obj}")
        
        # Create a future and store it so the `communicate_with_peer` can set it
        response_future = asyncio.Future()
        if port not in self.response_futures:
            self.response_futures[port] = []
        self.response_futures[port].append(response_future)

        # Wait for the response future to be completed by `communicate_with_peer`
        response = await response_future
        return response
    
    async def search_for_file(self,file_name):
        for peer_port in self.peers.keys():
            msg_obj = await self.send_message(peer_port, "Query", file_name, 2)
            if(msg_obj.message_type=="QueryHIT"):
                print(f"Query to {peer_port} successful {msg_obj}")
                await self.download_file(file_name,msg_obj.file_server_port)
                break
            else:
                print(f"Query to {peer_port} unsuccessful {msg_obj}")

    async def download_file(self,file_name, port,):
        # Create output directory if it doesn't exist
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        output_file_path = os.path.join(self.directory, file_name)

        # Connect to the file server
        reader, writer = await asyncio.open_connection(HOST, port)

        # Request the file from the server
        writer.write(file_name.encode())
        await writer.drain()  # Ensure the request is sent

        # Open a file to write the downloaded content
        with open(output_file_path, 'wb') as output_file:
            while True:
                data = await reader.read(1024)  # Read in chunks
                if not data:
                    break
                output_file.write(data)  # Write data to file
                print(f"Received {len(data)} bytes.")

        print(f"\nFile '{file_name}' downloaded to '{output_file_path}'.")
        writer.close()
        await writer.wait_closed()

async def connect_to_server(server_port,my_port,speed):
    try:
        reader, writer = await asyncio.open_connection(HOST, server_port)
        print(f"Connected to bootstrap server at {HOST}:{server_port}")

        # Send speed to the bootstrap server
        writer.write(f"{speed}:{my_port}\n".encode())
        await writer.drain()

        # Receive list of peers from the bootstrap server
        data = await reader.read(4096)
        # Close connection to the bootstrap server
        writer.close()
        await writer.wait_closed()
        peers_list = data.decode().splitlines()

        if not peers_list:
            print("No peers received from the bootstrap server.")
            return []

        print(f"Received list of peers from the bootstrap server: {peers_list}")

        # Parse the list and return the peers with their speeds
        initial_peers = [int(peer)for peer in peers_list]
        return initial_peers

    except ConnectionRefusedError:
        print(f"Connection to {HOST}:{port} failed")
        return []
    except Exception as e:
        print(f"An error occurred while connecting to the server: {e}")
        return []

async def main():
    args = sys.argv[1:]
    print(args)
    hasPort = False
    targetPort = None
    directory = None
    hasSpeed = False
    speed = 100  # Default speed if not specified

    for arg in args:
        if arg == "-s":
            hasSpeed = True
        elif hasSpeed:
            speed = int(arg)
            hasSpeed = False
        elif arg == "-p":
            hasPort = True
        elif hasPort:
            targetPort = int(arg)
            hasPort = False
        else:
            directory = arg

    print(f"Directory: {directory}")
    if directory:
        if not os.path.isdir(directory):
            os.makedirs(directory)
        log_file_path = os.path.join(directory, "output.log")
        with open(log_file_path, 'w') as log_file:
            pass  # This ensures the file is created

    port = targetPort if targetPort else 12345
    peer = GnutellaPeer(port, speed, directory)

    # Start the server to listen for incoming connections
    asyncio.create_task(peer.start_server())
    asyncio.create_task(peer.start_file_server())
    # Connect to the bootstrap server at a known IP and port
    bootstrap_port = 9000  # Example port for the bootstrap server

    available_peers = await connect_to_server(bootstrap_port,port,speed)
    if available_peers:
        print("Available peers:", available_peers)
        await peer.connect_to_initial_peers(available_peers)
        peer.print_connected_peers()  # Print connected peers after connecting

        # Example of sending a message to a specific peer
        # if available_peers:
        #     first_peer_address = available_peers[0]  # Taking the first peer for example
        #     await peer.send_message(first_peer_address, "SYN", "Hello, Peer!", ttl=5, port=port)

    else:
        print("No available peers found.")
    # if(directory!="kiran"):
    #     file_name="sai.jpg"
    #     await search_for_file(peer,file_name,peer.port)
    if(directory!="kiran"):
        file_name="sai.jpeg"
        await peer.search_for_file(file_name)
    await asyncio.Event().wait()

if __name__ == "__main__":
    asyncio.run(main())