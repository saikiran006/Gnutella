import asyncio

# Dictionary to store active peers and their speeds with port as the key
active_peers = {}

async def handle_client(reader, writer):
    peer_addr = writer.get_extra_info('peername')
    print(f"Peer connected: {peer_addr}")

    # Read the initial data (assuming it's the speed and port sent by the peer)
    data = await reader.readline()
    try:
        data = data.decode().strip()
        speed, port = map(int, data.split(':'))
        active_peers[port] = speed 
        print(f"Received speed and port from {peer_addr}: Speed={speed}, Port={port}")
    except ValueError:
        # Handle invalid data format
        print(f"Invalid data received from {peer_addr}")
        writer.write(b"Invalid data format\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    # Send the list of known peers (port numbers only) to the newly connected peer
    print(f"active_peers: {active_peers}")
    if len(active_peers) > 1:
        known_ports = [str(p) for p in active_peers.keys() if p != port]
        writer.write("\n".join(known_ports).encode() + b'\n')
        await writer.drain()
    else:
        # Send an empty line if there are no other peers
        writer.write(b"\n")  # Corrected to just write an empty byte string followed by a newline
        await writer.drain()

    # Keep the connection open for a short while and close it
    await asyncio.sleep(1)
    writer.close()
    await writer.wait_closed()

async def main():
    server = await asyncio.start_server(handle_client, '127.0.0.1', 9000)
    print("Bootstrap server running on 127.0.0.1:9000")
    async with server:
        await server.serve_forever()

if __name__ == "__main__":
    asyncio.run(main())
