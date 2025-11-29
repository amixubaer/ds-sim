#!/usr/bin/env python3

import socket
import sys
import argparse

HOST = "localhost"
PORT = 50000
USERNAME = "scheduler"

def send(sock, msg):
    """Transmit ASCII text through socket"""
    sock.sendall(msg.encode())

def receive(sock, timeout=5):
    """Read incoming data with timeout, return trimmed ASCII text or empty if none"""
    sock.settimeout(timeout)
    try:
        data = b""
        while True:
            chunk = sock.recv(1)
            if not chunk:
                break
            data += chunk
            if data.endswith(b"\n"):
                break
    except socket.timeout:
        return ""
    finally:
        sock.settimeout(None)
    return data.decode().strip()

def choose_server(servers, need_cores, need_mem, need_disk):
    """
    Tiered Fit Priority Scheduler (TFPS)
    """
    # 1. Filter only servers that CAN run the job
    valid = []
    for server in servers:
        if (server['cores'] >= need_cores and 
            server['memory'] >= need_mem and 
            server['disk'] >= need_disk):
            valid.append(server.copy())  # Copy to avoid modifying original
    
    if not valid:
        return None

    # 2. Compute "core gap" = cores - need_cores
    for server in valid:
        server['core_gap'] = server['cores'] - need_cores

    # 3. Group servers into buckets by core gap (smallest gap = highest priority)
    valid.sort(key=lambda s: s['core_gap'])

    # 4. Find smallest core gap available
    best_gap = valid[0]['core_gap']

    # 5. Keep only servers with best_gap
    top_tier = [s for s in valid if s['core_gap'] == best_gap]

    # 6. From this tier, select one with highest memory
    top_tier.sort(key=lambda s: s['memory'], reverse=True)
    highest_mem = top_tier[0]['memory']
    mem_tier = [s for s in top_tier if s['memory'] == highest_mem]

    # 7. Break ties by state: prefer active/idle
    priority_states = ["active", "idle", "booting", "inactive", "unavailable"]
    state_ranks = {state: i for i, state in enumerate(priority_states)}
    mem_tier.sort(key=lambda s: state_ranks.get(s['state'], 999))

    # 8. Final tie breaker → fewest waiting jobs
    mem_tier.sort(key=lambda s: s.get('waiting_jobs', 0))

    return mem_tier[0]  # best choice

def parse_server_line(line):
    """Parse server state line into structured fields"""
    parts = line.split()
    if len(parts) < 9:
        return None
    
    server = {
        'type': parts[0],
        'id': parts[1],
        'state': parts[2],
        'cur_start_time': parts[3],
        'cores': int(parts[4]),
        'memory': int(parts[5]),
        'disk': int(parts[6]),
        'waiting_jobs': int(parts[7]),
        'running_jobs': int(parts[8])
    }
    
    # Handle additional fields if present (for failures)
    if len(parts) > 9:
        server['failures'] = int(parts[9])
        server['total_fail_time'] = int(parts[10])
        # ... other failure fields can be added as needed
    
    return server

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--algo",
        default=USERNAME,
        help="Scheduling algorithm name (sent in AUTH).",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=PORT,
        help="Port ds-server is listening on (default 50000).",
    )
    args = parser.parse_args()

    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, args.port))
    
    print(f"Connected to {HOST}:{args.port}", file=sys.stderr)

    # Perform HELO → AUTH handshake
    send(sock, "HELO\n")
    response = receive(sock)
    if response != "OK":
        print(f"HELO failed: {response}", file=sys.stderr)
        sock.close()
        return

    send(sock, f"AUTH {args.algo}\n")
    response = receive(sock)
    if response != "OK":
        print(f"AUTH failed: {response}", file=sys.stderr)
        sock.close()
        return

    print("Handshake successful", file=sys.stderr)

    # Main loop
    while True:
        send(sock, "REDY\n")
        message = receive(sock)

        if not message:
            print("No response from server", file=sys.stderr)
            break

        if message == "NONE":
            print("No more jobs - simulation complete", file=sys.stderr)
            send(sock, "QUIT\n")
            receive(sock)
            break

        if message.startswith("JOBN") or message.startswith("JOBP"):
            # Parse job requirements
            parts = message.split()
            if len(parts) < 7:
                print(f"Invalid job message: {message}", file=sys.stderr)
                continue

            submit_time = parts[1]
            job_id = parts[2]
            cores = int(parts[3])
            memory = int(parts[4])
            disk = int(parts[5])
            est_runtime = parts[6]

            print(f"Processing {parts[0]}: job_id={job_id}, cores={cores}, memory={memory}, disk={disk}", file=sys.stderr)

            # Get capable servers
            send(sock, f"GETS Capable {cores} {memory} {disk}\n")
            header = receive(sock)

            if not header.startswith("DATA"):
                print(f"GETS failed: {header}", file=sys.stderr)
                continue

            # Parse number of server records
            n_recs = int(header.split()[1])
            rec_len = int(header.split()[2])

            send(sock, "OK\n")

            # Read N server records
            server_lines = []
            for _ in range(n_recs):
                line = receive(sock)
                if line:
                    server_lines.append(line)

            send(sock, "OK\n")

            # Wait for final "."
            dot_line = receive(sock)
            while dot_line != ".":
                # Sometimes there might be additional data, read until we get "."
                additional_line = receive(sock)
                if not additional_line:
                    break
                dot_line = additional_line

            # Parse server lines into structured data
            parsed_servers = []
            for line in server_lines:
                server = parse_server_line(line)
                if server:
                    parsed_servers.append(server)

            print(f"Found {len(parsed_servers)} capable servers", file=sys.stderr)

            # Choose best server using TFPS algorithm
            choice = choose_server(parsed_servers, cores, memory, disk)

            if choice:
                # Schedule to best server
                cmd = f"SCHD {job_id} {choice['type']} {choice['id']}\n"
                print(f"Scheduling to: {choice['type']} {choice['id']} (cores: {choice['cores']}, mem: {choice['memory']}, state: {choice['state']})", file=sys.stderr)
                send(sock, cmd)
            else:
                # Fallback: use first capable server
                if parsed_servers:
                    first = parsed_servers[0]
                    cmd = f"SCHD {job_id} {first['type']} {first['id']}\n"
                    print(f"Fallback scheduling to: {first['type']} {first['id']}", file=sys.stderr)
                    send(sock, cmd)
                else:
                    print("No capable servers found!", file=sys.stderr)
                    # Emergency fallback - try to schedule to any server
                    send(sock, f"SCHD {job_id} unknown 0\n")

            # Get scheduler acknowledgement
            ack = receive(sock)
            if ack != "OK":
                print(f"SCHD failed: {ack}", file=sys.stderr)

        elif message.startswith("CHKQ"):
            print("Check queue event - ending simulation", file=sys.stderr)
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            receive(sock)
            break

        elif message.startswith(("JCPL", "RESF", "RESR")):
            # Handle other events without response
            print(f"Ignoring event: {message}", file=sys.stderr)
            continue

        else:
            print(f"Unknown message: {message}", file=sys.stderr)

    # Close connection
    sock.close()
    print("Connection closed", file=sys.stderr)

if __name__ == "__main__":
    main()