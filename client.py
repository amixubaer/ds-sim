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

def choose_server_tfps(servers, need_c, need_m, need_d):
    """
    Tiered Fit Priority Scheduler (optimized version):
    1. Filter capable servers
    2. Find smallest core gap
    3. Pick highest memory in that gap
    4. Prioritize state: active > idle > booting > inactive
    5. Break ties with waiting jobs
    """
    # 1. Filter capable servers
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # 2. Compute core gap and find smallest
    for s in eligible:
        s["core_gap"] = s["cores"] - need_c

    eligible.sort(key=lambda x: x["core_gap"])
    best_gap = eligible[0]["core_gap"]

    # 3. Keep only servers with best gap
    gap_group = [s for s in eligible if s["core_gap"] == best_gap]

    # 4. From this tier, pick highest memory
    gap_group.sort(key=lambda x: x["memory"], reverse=True)
    max_mem = gap_group[0]["memory"]
    mem_group = [s for s in gap_group if s["memory"] == max_mem]

    # 5. Break ties by state
    state_priority = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}
    mem_group.sort(key=lambda x: state_priority.get(x["state"], 99))

    # 6. Final tie breaker: fewest waiting jobs
    mem_group.sort(key=lambda x: x["waiting"])

    return mem_group[0]

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=5)
    except Exception:
        return

    # Handshake with proper newlines
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

            # Choose server with TFPS
            selected = choose_server_tfps(servers, req_cores, req_mem, req_disk)

            if selected is None and servers:
                # Fallback to first server
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