#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    if not msg.endswith('\n'):
        msg += '\n'
    sock.sendall(msg.encode())

def recv_line(sock):
    data = b""
    while not data.endswith(b'\n'):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def parse_server(line):
    parts = line.split()
    if len(parts) < 8:
        return None
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7])
    }

def choose_server_optimized(servers, need_c, need_m, need_d):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    candidates = []
    for s in eligible:
        # Core utilization (for resource utilization metric)
        core_util = need_c / s["cores"] if s["cores"] > 0 else 0
        
        # Queue impact (for turnaround metric)
        queue_impact = s["waiting"] * 100
        
        # State impact (for cost/turnaround)
        state_impact = {"active": 0, "idle": 50, "booting": 150, "inactive": 300}.get(s["state"], 500)
        
        # Balanced score
        total_score = (
            (1 - core_util) * 40 +  # Prefer higher utilization
            queue_impact * 0.4 +    # Prefer shorter queues
            state_impact * 0.2      # Prefer ready servers
        )
        
        candidates.append((total_score, s))

    # Sort for best overall performance
    candidates.sort(key=lambda x: (
        x[0],                        # Balanced score
        x[1]["waiting"],             # Turnaround: fewer waiting
        -x[1]["cores"],              # Utilization: more cores
        x[1]["id"],                  # Consistency: lower ID
    ))

    return candidates[0][1]

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=5)
    except Exception:
        return

    # Handshake
    send(sock, "HELO\n")
    if recv_line(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    if recv_line(sock) != "OK":
        sock.close()
        return

    # Main event loop
    while True:
        send(sock, "REDY\n")
        msg = recv_line(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT\n")
            recv_line(sock)
            break

        parts = msg.split()

        # Handle job scheduling
        if parts[0] in ["JOBN", "JOBP"] and len(parts) >= 7:
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

            # Get capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}\n")
            header = recv_line(sock)

            if not header.startswith("DATA"):
                continue

            count = int(header.split()[1])
            send(sock, "OK\n")

            # Read server records
            servers = []
            for _ in range(count):
                line = recv_line(sock)
                server = parse_server(line)
                if server:
                    servers.append(server)

            send(sock, "OK\n")

            # Read until "."
            while recv_line(sock) != ".":
                pass

            # Choose server
            selected = choose_server_optimized(servers, req_cores, req_mem, req_disk)

            if selected is None and servers:
                selected = servers[0]

            if selected:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
                recv_line(sock)

        # Handle check queue
        elif parts[0] == "CHKQ":
            send(sock, "OK\n")
            recv_line(sock)
            send(sock, "QUIT\n")
            recv_line(sock)
            break

    sock.close()

if __name__ == "__main__":
    main()