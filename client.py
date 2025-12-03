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

def choose_server_final(servers, need_c, need_m, need_d):
    """
    Optimized TFPS with runtime awareness:
    - For high-load situations, be more aggressive with server selection
    - Consider waiting jobs more heavily
    """
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # Score each server
    candidates = []
    for s in eligible:
        # Core gap (smaller is better)
        core_gap = s["cores"] - need_c
        
        # State priority
        state_score = {
            "active": 0,
            "idle": 1,
            "booting": 2,
            "inactive": 3
        }.get(s["state"], 4)
        
        # Total score = core_gap + state_priority + waiting_jobs
        # Weights: core_gap (40%), state (30%), waiting (30%)
        total_score = (
            core_gap * 0.4 +
            state_score * 0.3 +
            s["waiting"] * 0.3
        )
        
        candidates.append((total_score, s))

    # Sort by score, then memory (higher better), then id
    candidates.sort(key=lambda x: (
        x[0],                        # Total score
        -x[1]["memory"],             # More memory
        x[1]["id"],                  # Lower ID
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
            selected = choose_server_final(servers, req_cores, req_mem, req_disk)

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