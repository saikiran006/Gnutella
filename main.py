import asyncio
import sys
import os
import time
from gnutella_peer import GnutellaPeer
from logger import Logger
import aioconsole

HOST = "127.0.0.1"

# Connect to the server and get peer list
async def connect_to_server(server_port, my_port, speed):
    try:
        reader, writer = await asyncio.open_connection(HOST, server_port)
        print(f"Connected to bootstrap server at {HOST}:{server_port}")

        # Send speed and port to the bootstrap server
        writer.write(f"{speed}:{my_port}\n".encode())
        await writer.drain()

        # Receive list of peers
        data = await reader.read(4096)
        writer.close()
        await writer.wait_closed()

        peers_list = data.decode().splitlines()

        if not peers_list:
            print("No peers received from the bootstrap server.")
            return []

        print(f"Received list of peers from the bootstrap server: {peers_list}")
        return [int(peer) for peer in peers_list]

    except ConnectionRefusedError:
        print(f"Connection to {HOST}:{server_port} failed")
        return []
    except Exception as e:
        print(f"An error occurred while connecting to the server: {e}")
        return []


# Directory setup
def setup_directory(directory):
    if directory:
        if not os.path.isdir(directory):
            os.makedirs(directory)
        log_file_path = os.path.join(directory, "output.log")
        with open(log_file_path, 'w'):
            pass  # Ensure the file is created


# Handle user input asynchronously
async def handle_user_input(peer):
    while True:
        file_name = await aioconsole.ainput("Enter the file name you want to download (or type 'exit' to quit): ")
        if file_name.lower() == 'exit':
            print("Shutting down...")
            break

        try:
            start_time = time.monotonic()
            response = await peer.search_for_file(file_name)
            end_time = time.monotonic()
            total_time = end_time - start_time
            print(f"Request completed in {total_time:.2f} seconds.")
            print(f"Response: {response}")
        except Exception as e:
            print(f"Error during file search: {e}")


# Run asynchronous tasks safely
async def run_task(task, name):
    try:
        await task
    except Exception as e:
        print(f"Error in {name}: {e}")


# Monitor active tasks for debugging
async def monitor_tasks():
    while True:
        print(f"Active tasks: {len(asyncio.all_tasks())}")
        await asyncio.sleep(5)


# Main function to run the program
async def main():
    args = sys.argv[1:]

    # Variables to store parsed command line arguments
    target_port = None
    directory = None
    speed = 100  # Default speed

    # Parsing command line arguments
    has_speed = False
    has_port = False

    for arg in args:
        if arg == "-s":
            has_speed = True
        elif has_speed:
            speed = int(arg)
            has_speed = False
        elif arg == "-p":
            has_port = True
        elif has_port:
            target_port = int(arg)
            has_port = False
        else:
            directory = arg

    # Setup the directory for logs
    setup_directory(directory)

    # Default port if not provided
    port = target_port if target_port else 12345
    peer = GnutellaPeer(port, speed, directory)

    # Start server and file server asynchronously
    asyncio.create_task(run_task(peer.start_server(), "start_server"))
    asyncio.create_task(run_task(peer.start_file_server(), "start_file_server"))

    # Bootstrap server configuration
    bootstrap_port = 9000  # Example bootstrap server port

    # Connect to bootstrap server and retrieve available peers
    available_peers = await connect_to_server(bootstrap_port, port, speed)
    if available_peers:
        print("Available peers:", available_peers)
        await peer.connect_to_initial_peers(available_peers)
        peer.print_connected_peers()  # Print connected peers after connecting
    else:
        print("No available peers found.")

    # Start user input handling
    asyncio.create_task(handle_user_input(peer))

    # Start task monitoring for debugging
    # asyncio.create_task(monitor_tasks())

    # Keep the event loop running
    await asyncio.Event().wait()


if __name__ == "__main__":
    asyncio.run(main())
