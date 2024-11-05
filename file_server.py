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
        
        # Receive filename from the client
        filename = await reader.read(1024)  # Read up to 1024 bytes
        filename = filename.decode().strip()  # Decode and strip whitespace
        
        # Construct full path
        full_path = os.path.join(self.directory, filename)
        
        print(f"Client requested file: {full_path}")
        
        if os.path.exists(full_path) and os.path.isfile(full_path):
            # Send file in chunks
            await self.send_file(writer, full_path)
        else:
            writer.write(f"File '{filename}' not found in directory '{self.directory}'.\n".encode())
            await writer.drain()
        
        print(f"Closing connection with {addr}.")
        writer.close()
        await writer.wait_closed()

    async def send_file(self, writer, filepath, chunk_size=1024):
        # writer.write(f"Sending file: {os.path.basename(filepath)}\n".encode())
        # await writer.drain()
        
        with open(filepath, 'rb') as f:
            while True:
                data = f.read(chunk_size)
                if not data:
                    break
                writer.write(data)
                await writer.drain()  # Ensure the data is sent
                print(f"Sent {len(data)} bytes of {os.path.basename(filepath)}.")

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
