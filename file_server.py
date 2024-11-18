import asyncio
import os
import socket

class File_Server:
    def __init__(self, host='localhost', directory='shared_files'):
        self.host = host
        self.port = self.get_random_port()
        self.directory = directory

        # Ensure the directory exists
        if not os.path.exists(self.directory):
            os.makedirs(self.directory)

    def get_random_port(self):
        """Select an available random port."""
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind((self.host, 0))  # Bind to port 0 to select an available port
            return s.getsockname()[1]  # Return the randomly selected port

    async def handle_client(self, reader, writer):
        addr = writer.get_extra_info('peername')
        print(f"Connection from {addr} has been established.")
        
        # Receive the request message which includes file name, chunk start, and chunk end
        request_msg = await reader.read(1024)  # Read up to 1024 bytes
        request_msg = request_msg.decode().strip()  # Decode and strip whitespace
        
        # Parse request message (expected format: file_name,chunk_start,chunk_end)
        file_name, chunk_start, chunk_end = request_msg.split(',')
        chunk_start, chunk_end = int(chunk_start), int(chunk_end)
        
        # Construct full path
        full_path = os.path.join(self.directory, file_name)
        
        print(f"Client requested file: {file_name} from byte {chunk_start} to {chunk_end}")
        
        if os.path.exists(full_path) and os.path.isfile(full_path):
            # Send file chunk in the requested range
            await self.send_chunk(writer, full_path, chunk_start, chunk_end)
        else:
            writer.write(f"File '{file_name}' not found in directory '{self.directory}'.\n".encode())
            await writer.drain()
        
        print(f"Closing connection with {addr}.")
        writer.close()
        await writer.wait_closed()

    async def send_chunk(self, writer, filepath, chunk_start, chunk_end, chunk_size=1024):
        # Open the file and seek to the start of the requested chunk
        with open(filepath, 'rb') as f:
            f.seek(chunk_start)
            offset = chunk_start
            # Move the file pointer to the starting position of the chunk
            while offset < chunk_end:
                rem_chunk = chunk_end - offset
                curr_chunk_size = min(chunk_size, rem_chunk)  # Ensure not to exceed remaining bytes
                data = f.read(curr_chunk_size)
                if data:
                    writer.write(data)  # Send the chunk to the client
                    await writer.drain()  # Ensure the data is sent
                    print(f"Sent {len(data)} bytes of {os.path.basename(filepath)} from byte {offset} to {offset + len(data)}.")
                    offset += len(data)  # Update the offset by the number of bytes sent
                else:
                    print(f"No data to send for the chunk from {offset} to {chunk_end}.")
                    break  # No more data to send, exit the loop


    async def start_server(self):
        server = await asyncio.start_server(
            self.handle_client, self.host, self.port)
        
        print(f"File Sharing Server started on port: {self.port}")

        async with server:
            await server.serve_forever()

if __name__ == "__main__":
    # Specify the directory for sharing files
    file_sharing_server = File_Server(directory='shared_files')
    asyncio.run(file_sharing_server.start_server())
