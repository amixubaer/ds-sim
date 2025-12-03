#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    sock.sendall(msg.encode("ascii"))

def receive(sock, timeout=2):
    sock.settimeout(timeout)
    try:
        data = sock.recv(8192)
        if not data:
            return ""
        return data.decode("ascii", errors="ignore").strip()
    except:
        return ""

def parse_server(line):
    parts = line.split()
    s = {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": 0,
        "running": 0,
    }
    if len(parts) >= 9:
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except ValueError:
            s["waiting"] = 0
            s["running"] = 0
    return s

def choose_server_optimized(servers, need_c, need_m, need_d, est_runtime):
    """
    Optimized for turnaround time:
    1. Filter capable servers
    2. Score = (waiting_time_estimate + state_penalty)
    3. waiting_time_estimate = (waiting + running) * est_runtime / cores
    4. State penalties: active(0), idle(1), booting(5), inactive(10)
    5. Prefer servers with HIGHER cores (for parallel execution)
    """
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # State penalties (lower is better)
    state_penalty = {
        "active": 0,      # Immediately available
        "idle": 1,        # Available but might need wake-up
        "booting": 5,     # Boot time penalty
        "inactive": 10,   # Boot time + activation penalty
    }

    candidates = []
    for s in eligible:
        # Estimate waiting time based on queue and cores
        if s["cores"] > 0:
            queue_time = (s["waiting"] + s["running"]) * est_runtime / s["cores"]
        else:
            queue_time = 1000000  # Large penalty if no cores (shouldn't happen)
        
        penalty = state_penalty.get(s["state"].lower(), 20)
        
        # Total score = queue_time + state_penalty
        total_score = queue_time + penalty
        
        candidates.append((total_score, s))

    # Sort by total_score (lower is better), then prefer more cores for parallelism
    candidates.sort(key=lambda x: (
        x[0],                        # Total score
        -x[1]["cores"],              # More cores (for parallel execution)
        x[1]["waiting"],             # Fewer waiting jobs
        x[1]["id"],                  # Lower server ID
    ))

    return candidates[0][1]

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except:
        return

    # Handshake 
    send(sock, "HELO\n")
    if receive(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    if receive(sock) != "OK":
        sock.close()
        return

    # Main event loop
    while True:
        send(sock, "REDY\n")
        msg = receive(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT\n")
            receive(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            # Request capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}\n")
            header = receive(sock)

            if not header.startswith("DATA"):
                continue

            count = int(header.split()[1])

            # First OK
            send(sock, "OK\n")

            # Read N server lines
            servers = []
            while len(servers) < count:
                chunk = receive(sock)
                if not chunk:
                    break
                for line in chunk.split("\n"):
                    line = line.strip()
                    if line:
                        servers.append(parse_server(line))
                        if len(servers) == count:
                            break

            # Second OK
            send(sock, "OK\n")

            # Wait for "."
            while True:
                endmsg = receive(sock)
                if "." in endmsg:
                    break

            # Choose server with optimized heuristic
            selected = choose_server_optimized(servers, req_cores, req_mem, req_disk, est_runtime)

            if selected is None:
                # fallback: just take first server in list
                f = servers[0]
                send(sock, f"SCHD {job_id} {f['type']} {f['id']}\n")
                receive(sock)
            else:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
                receive(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            break

        else:
            continue

    sock.close()

if __name__ == "__main__":
    main()