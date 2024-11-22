# gnutella_peer.py
import asyncio
import os
import random
import sys
import json
from typing import List
from entities import Message, Peer
from file_server import File_Server
from logger import Logger


MIN_CONNS = 3
MAX_CONNS = 10
HOST = "127.0.0.1"

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
        self.server=None
        self.query_msg_ids=set()
        log_file_path = self.directory+"/output.log"  # Specify your log file path
        self.logger = Logger(log_file_path)
        self.logger.write("logging started")
        self.file_server=File_Server(self.logger,directory=directory)

        # Redirect standard output to the logger
        # sys.stdout = self.logger

    def read_files_in_directory(self):
        for filename in os.listdir(self.directory):
        # Create full path
            full_path = os.path.join(self.directory, filename)
            
            # Check if it is a file
            if os.path.isfile(full_path):
                self.files.append(filename)
    
    async def start_file_server(self):
        await self.file_server.start_server()
        self.logger.write("File Server Started on",self.file_server.port)

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
        self.logger.write("Currently connected peers:")
        for peer in self.peers.values():
            self.logger.write(str(peer.peer_port))

    async def start_server(self):
        """Start the server and begin accepting connections."""
        try:
            self.server = await asyncio.start_server(self.handle_connection, '127.0.0.1', self.port)
            self.logger.write(f"Sharing Server started on {self.server.sockets[0].getsockname()}")
            
            # Run the server in an asynchronous context
            async with self.server:
                await self.server.serve_forever()
        except Exception as e:
            self.logger.write(f"Error starting the server on port {self.port}: {e}")

    async def stop_server(self):
        """Stop the file server gracefully."""
        if self.server:  # Check if the server is running
            try:
                self.logger.write(f"Stopping Sharing Server on port {self.port}")
                self.server.close()  # Close the server
                await self.server.wait_closed()  # Wait until all connections are closed
                self.logger.write("Sharing Server stopped.")
            except Exception as e:
                self.logger.write(f"Error while stopping the server on port {self.port}: {e}")
        else:
            self.logger.write("Server is not running, cannot stop.")


    async def connect_to_initial_peers(self, initial_peers):
        """ Connect to all initial peers based on the provided list. """
        if initial_peers:
            for peer in initial_peers:
                # Connect to each peer in the initial_peers list
                await self.connect_to_peer(peer)
                await self.send_message(peer, "SYN", "Hello, Peer!", ttl=5)
                print(f"Connected to peer: {peer}")
    
    async def connect_to_peer(self, port):
        """Connects to a peer and handles communication."""
        try:
            reader, writer = await asyncio.open_connection(HOST, port)
            self.logger.write(f"Connected to peer at {HOST}:{port}")

            # Store the peer in the dictionary
            self.peers[port] = Peer(port, self.speed, reader, writer)
            self.messages[port] = []  # Initialize message list for the peer
            self.response_futures[port] = []

            # Start communication loop
            asyncio.create_task(self.communicate_with_peer(port))

        except ConnectionRefusedError:
            self.logger.write(f"Failed to connect to {port}")
            return None, None  # Return None if connection fails
        except asyncio.CancelledError:
            self.logger.write(f"Connection to {port} cancelled")
            return None, None  # Return None on cancellation
        except Exception as e:
            self.logger.write(f"Error during communication with {port}: {e}")
            return None, None  # Return None on other errors
    
    async def communicate_with_peer(self, port):
        """ Send a message to a specific peer and wait for a response. """
        peer = self.peers.get(port)

        if not peer:
            self.logger.write(f"Peer {port} not found.")
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
                        self.logger.write(f"Sent message to {port}: {msg_obj}")

                        if msg_obj.message_type == "ACK":
                            continue

                        # Wait for a response from the peer
                        data = await peer.reader.read(4096)  # Read from the peer
                        if data:
                            json_message = data.decode().strip()
                            try:
                                received_msg = Message.from_json(json_message)
                                self.logger.write(f"Received from {port}: {received_msg}")

                                # Notify the response future
                                if port in self.response_futures and self.response_futures[port]:
                                    response_future = self.response_futures[port].pop(0)
                                    if not response_future.done():
                                        response_future.set_result(received_msg)

                            except json.JSONDecodeError:
                                self.logger.write(f"Invalid message format from {port}")
                    else:
                        self.logger.write(f"Peer {port} writer not available.")

                await asyncio.sleep(1)

        except Exception as e:
            self.logger.write(f"Error in communication with peer {port}: {e}")
    
    async def handle_connection(self, reader, writer):
        addr = writer.get_extra_info('peername')
        port = addr[1]  # Extract the port number from the address
        self.logger.write(f"Connected to port: {port}")  # Logging the port number only
        # self.peers[port] = Peer(port, self.speed, reader, writer)  # Store the peer using port number

        try:
            while True:
                data = await reader.read(4096)
                if not data:
                    break  # Connection closed by the peer

                json_message = data.decode().strip()
                self.logger.write(f"Received raw data from port {port}: {json_message}")  # Log raw data received
                response_message = None
                try:
                    received_msg = Message.from_json(json_message)
                    if received_msg.message_type == "SYN":
                        await self.connect_to_peer(received_msg.port)
                        msg_obj = Message(
                            message_id=self.get_message_id(),
                            message_type="ACK",
                            message="Connected Successfully",
                            ttl=1,
                            port=self.port
                        )
                        response_message = msg_obj.to_json().encode()

                    elif received_msg.message_type == "Query":
                        response_message = await self.handle_query(received_msg)

                    elif received_msg.message_type == "DISCONNECT":
                        # Handle disconnection from the peer
                        self.logger.write(f"Received DISCONNECT message from port {received_msg.port}")
                        await self.disconnect_from_peer(received_msg.port)

                    else:
                        self.logger.write(f"Unknown message type: {received_msg.message_type}")  # Log unknown message types
                except json.JSONDecodeError:
                    self.logger.write(f"Invalid message format from port {port}: {json_message}")  # More detailed error logging
                except Exception as e:
                    self.logger.write(f"An error occurred: {e}")

                if response_message:
                    writer.write(response_message + b'\n')  # Ensure response_message is encoded properly
                    await writer.drain()

        except asyncio.CancelledError:
            # Handle disconnection when the server is shutting down or connection is forcibly closed
            self.logger.write(f"Connection with port {port} was cancelled.")
        except Exception as e:
            self.logger.write(f"Error during connection handling: {e}")
        finally:
            # Clean up and close the connection if not already done
            self.logger.write(f"Closing connection to port {port}")
            writer.close()
            await writer.wait_closed()
            if port in self.peers:
                del self.peers[port]  # Remove the peer from the peers dictionary
            self.logger.write(f"Peer at port {port} disconnected and removed.")
    # Remove the peer from the dictionary using the port
        
    async def handle_query(self, received_msg):
        self.logger.write(f"In handle_query: Received message {received_msg}")
        msg_id = received_msg.message_id
        file_name = received_msg.message
        TTL = received_msg.ttl
        print("recvd msg id", msg_id)
        print("msg_ids saved", self.query_msg_ids)
        
        # Check if we've already processed this message
        if msg_id in self.query_msg_ids:
            self.logger.write(f"Message {msg_id} already processed. Sending QueryFAIL.")
            msg_obj = Message(
                message_id=self.get_message_id(),
                message_type="QueryFAIL",
                message=f"File '{file_name}' not found in the network",
                ttl=1,
                port=self.port
            )
            return msg_obj.to_json().encode()

        # Mark message ID as processed
        self.query_msg_ids.add(msg_id)
        peer_fs_ports = set()  # Use a set for unique ports
        file_size = None

        # Check if the file exists locally
        if file_name in self.files:
            self.logger.write("File found locally.")
            peer_fs_ports.add(self.file_server.port)  # Add to the set
            file_path = os.path.join(self.directory, file_name)
            file_size = os.stat(file_path).st_size
            self.logger.write(f"Local file size: {file_size} bytes")
        else:
            self.logger.write(f"File '{file_name}' not found locally. TTL remaining: {TTL}")

        # Forward the query if TTL > 1
        if TTL > 1:
            new_ttl = TTL - 1
            for peer_address in self.peers.keys():
                if peer_address == received_msg.port:
                    continue
                self.logger.write(f"Forwarding query for '{file_name}' to peer {peer_address} with TTL={new_ttl}")
                response = await self.send_message(peer_address, "Query", file_name, new_ttl, message_id=msg_id)

                # Collect file server ports from peers with QueryHIT responses
                if response.message_type == "QueryHIT":
                    if file_size is None:
                        file_size = response.file_size
                    peer_fs_ports.update(response.file_server_ports)  # Add multiple ports to the set

        # Return QueryHIT if any file server has the file
        if len(peer_fs_ports) > 0:
            self.logger.write(f"File '{file_name}' found on peers: {peer_fs_ports}")
            msg_obj = Message(
                message_id=self.get_message_id(),
                message_type="QueryHIT",
                message=f"File '{file_name}' found at peer file servers",
                file_size=file_size,
                ttl=1,
                port=self.port
            )
            msg_obj.file_server_ports = list(peer_fs_ports)  # Convert the set to a list for serialization
            return msg_obj.to_json().encode()
        else:
            # No file found in network
            self.logger.write(f"File '{file_name}' not found in the network.")
            msg_obj = Message(
                message_id=self.get_message_id(),
                message_type="QueryFAIL",
                message=f"File '{file_name}' not found in the network",
                ttl=1,
                port=self.port
            )
            return msg_obj.to_json().encode()

    async def send_message(self, port, message_type, message, ttl, message_id=None):
        """ Add a structured message to the queue for a specific peer. """
        if message_id is None:
            message_id = self.get_message_id()
        if(message_type == "Query"):
            self.query_msg_ids.add(message_id)
        msg_obj = Message(message_id=message_id, message_type=message_type, message=message, ttl=ttl, port=self.port)
        
        if port not in self.messages:
            self.messages[port] = []
        self.messages[port].append(msg_obj)
        self.logger.write(f"Added message for {port}: {msg_obj}")
        
        # Create a future and store it so the `communicate_with_peer` can set it
        response_future = asyncio.Future()
        if port not in self.response_futures:
            self.response_futures[port] = []
        self.response_futures[port].append(response_future)

        # Wait for the response future to be completed by `communicate_with_peer`
        response = await response_future
        return response
    
    async def search_for_file(self, file_name):
        successful_responses = []
        for peer_port in self.peers.keys():
            try:
                new_msg_id=self.get_message_id()
                msg_obj = await self.send_message(peer_port, "Query", file_name, 5,new_msg_id)
                if msg_obj.message_type == "QueryHIT":
                    self.logger.write(f"Query to {peer_port} successful: {msg_obj}")
                    successful_responses.append((msg_obj.file_server_ports, msg_obj.file_size))
                else:
                    self.logger.write(f"Query to {peer_port} unsuccessful: {msg_obj}")
            except Exception as e:
                self.logger.write(f"Error querying peer {peer_port}: {e}")

        if successful_responses:
            self.logger.write(f"Found file '{file_name}' on {len(successful_responses)} peers. Starting parallel downloads.")
            
            # Combine all available file server ports and download
            file_server_ports = set()
            file_size = 0
            for ports, size in successful_responses:
                file_server_ports.update(ports)
                file_size = size  # Assuming all responses have the same file size

            await self.parallel_download(file_name, list(file_server_ports), file_size)
            self.logger.write(f"Download of '{file_name}' complete!")
            self.files.append(file_name)
            return f"Success: File '{file_name}' downloaded from {len(file_server_ports)} peers."
        else:
            self.logger.write(f"File '{file_name}' not found on any peers.")
            return f"Failure: File '{file_name}' not found in the network."


    async def parallel_download(self, file_name, file_server_ports, file_size):
        download_tasks = []
        num_chunks = len(file_server_ports)
        base_chunk_size = file_size // num_chunks
        remainder = file_size % num_chunks
        next_chunk_start = 0

        for i, port in enumerate(file_server_ports):
            # Adjust the chunk size only for the last chunk if there's a remainder
            chunk_size = base_chunk_size + (1 if i < remainder else 0)
            
            # Append the download task for this chunk
            download_tasks.append(self.download_chunk(file_name, port, next_chunk_start, next_chunk_start + chunk_size))
            
            # Update the starting point for the next chunk
            next_chunk_start += chunk_size

        # Run all download tasks concurrently
        await asyncio.gather(*download_tasks)


    async def download_chunk(self, file_name, port, chunk_start, chunk_end):
        # Create output directory if it doesn't exist
        self.logger.write("fileName ", file_name)
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

        output_file_path = os.path.join(self.directory, file_name)

        # Connect to the file server
        reader, writer = await asyncio.open_connection(HOST, port)

        # Request the chunk from the server
        request_msg = f"{file_name},{chunk_start},{chunk_end}"
        writer.write(request_msg.encode())
        await writer.drain()  # Ensure the request is sent

        # Open a file to write the downloaded content
        with open(output_file_path, 'wb') as output_file:
            output_file.seek(chunk_start)
            msg_size=45*1024
            while True:
                data = await reader.read(msg_size)  # Read in chunks
                if not data:
                    break
                output_file.write(data)  # Write data to the file
                # print(f"Received {len(data)} bytes from peer at port {port}.")
                # await asyncio.sleep(0.1)  

        self.logger.write(f"\nFile '{file_name}' chunk downloaded from peer {port} to '{output_file_path}'.")
        writer.close()
        await writer.wait_closed()

    async def disconnect_from_peers(self):
        """Disconnect from all connected peers and notify them."""
        self.logger.write("Disconnecting from all peers...")

        # Create a DISCONNECT message
        disconnect_message = Message(
            message_id=self.get_message_id(),
            message_type="DISCONNECT",
            message="Goodbye from the peer",
            ttl=1,
            port=self.port
        )
        try:
            # Iterate over a copy of the peers' keys
            for peer_port in list(self.peers.keys()):
                peer = self.peers[peer_port]
                if peer.writer:  # Ensure the peer's writer is available for communication
                    try:
                        # Send the DISCONNECT message
                        peer.writer.write(disconnect_message.to_json().encode() + b'\n')
                        await peer.writer.drain()
                        self.logger.write(f"Sent DISCONNECT message to {peer_port}")

                        # Close the connection
                        peer.writer.close()
                        await peer.writer.wait_closed()
                        self.logger.write(f"Closed connection to {peer_port}")

                    except Exception as e:
                        self.logger.write(f"Error disconnecting from {peer_port}: {e}")

            # After disconnecting from all peers, clear the peers dictionary
            self.peers.clear()
            self.logger.write("Disconnected from all peers.")
        except Exception as e:
            self.logger.write(f"Error during disconnection process: {e}")

    async def disconnect_from_peer(self, port):
        """Disconnect from a single peer and close the connection."""
        if port in self.peers:
            peer = self.peers[port]
            if peer.writer:
                try:
                    # Close the connection
                    peer.writer.close()
                    await peer.writer.wait_closed()
                    self.logger.write(f"Successfully disconnected from peer {port}")
                except Exception as e:
                    self.logger.write(f"Error disconnecting from peer {port}: {e}")

            # Remove the peer from the connected peers list
            del self.peers[port]
            self.logger.write(f"Peer at port {port} removed from connected peers.")


        
