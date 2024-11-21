import asyncio
import math

# Dictionaries to store active peers categorized by their magnitude
low_peers = {}
med_peers = {}
high_peers = {}

num_connections = {}

MIN_CONNS = 2

async def handle_client(reader, writer):
    peer_addr = writer.get_extra_info('peername')
    print(f"Peer connected: {peer_addr}")

    try:
        # Read the initial data sent by the peer
        data = await reader.readline()
        data = data.decode().strip()

        # Parse the data (expecting bandwidth, cpu, ram, my_port, server_port)
        bandwidth, cpu, ram, my_port, server_port = map(int, data.split(':'))

        # Calculate the magnitude using the formula sqrt(bandwidth^2 + cpu^2 + ram^2)
        magnitude = math.sqrt(bandwidth**2 + cpu**2 + ram**2)
        print(f"Received data from {peer_addr}: server_port={server_port}, my_port={my_port}, "
              f"bandwidth={bandwidth}, cpu={cpu}, ram={ram}, magnitude={magnitude}")

        # Prepare the peer data
        peer_data = {
            "bandwidth": bandwidth,
            "cpu": cpu,
            "ram": ram,
        }

        # Categorize the peer based on the magnitude
        if magnitude < 2:
            curr_class = "low"
            low_peers[my_port] = peer_data
            eligible_peers = set(med_peers.keys())
        elif 2 <= magnitude < 4:
            curr_class = "medium"
            med_peers[my_port] = peer_data
            eligible_peers = set(high_peers.keys())
        else:
            curr_class = "high"
            high_peers[my_port] = peer_data
            eligible_peers = set(high_peers.keys()) - {my_port}

        print(f"Current class: {curr_class}")
        print(f"Eligible peers: {eligible_peers}")

        num_connections[my_port] = 0  # Initialize connections for the new peer

        # Filter and sort peers by number of connections (ascending)
        peers_to_send = sorted(
            ((key, value) for key, value in num_connections.items() if key in eligible_peers),
            key=lambda item: item[1]  # Sort by number of connections
        )

        # Select up to MIN_CONNS peers, excluding the current peer
        known_peers = [str(key) for key, _ in peers_to_send][:MIN_CONNS]

        # If not enough peers, expand selection to the closest class
        if len(known_peers) < MIN_CONNS:
            print(f"Not enough peers in the eligible class. Adding from closest class...")

            if curr_class == "low":
                additional_peers = sorted(
                    ((key, value) for key, value in num_connections.items() if key in med_peers),
                    key=lambda item: item[1]
                )
            elif curr_class == "medium":
                additional_peers = sorted(
                    ((key, value) for key, value in num_connections.items() if key in low_peers),
                    key=lambda item: item[1]
                )
            else:  # "high"
                additional_peers = sorted(
                    ((key, value) for key, value in num_connections.items() if key in med_peers),
                    key=lambda item: item[1]
                )

            # Add enough peers to reach MIN_CONNS
            for key, _ in additional_peers:
                if len(known_peers) >= MIN_CONNS:
                    break
                if str(key) not in known_peers:
                    known_peers.append(str(key))
        for key in known_peers:
            num_connections[key] = num_connections.get(key, 0) + 1
        print(f"Updated active peers: Low={len(low_peers)}, Med={len(med_peers)}, High={len(high_peers)}")
        print(f"Sending peers to {my_port}: {known_peers}")

        # Send the list of known peers to the newly connected peer
        if known_peers:
            writer.write("\n".join(known_peers).encode() + b'\n')
        else:
            writer.write(b"\n")  # Send an empty line if no peers are available
        await writer.drain()

    except ValueError:
        # Handle invalid data format
        print(f"Invalid data received from {peer_addr}")
        writer.write(b"Invalid data format\n")
        await writer.drain()
        writer.close()
        await writer.wait_closed()
        return

    except Exception as e:
        # General exception handling
        print(f"An error occurred with peer {peer_addr}: {e}")
        writer.close()
        await writer.wait_closed()
        return

    # Keep the connection open briefly before closing
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
