import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

# ---------------------------------------------------------
# Basic network helpers
# ---------------------------------------------------------

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


# ---------------------------------------------------------
# TFPS Server Selection Logic
# ---------------------------------------------------------

def parse_server(line):
    parts = line.split()
    # Expected: type id state curStartTime core memory disk waitingJobs …
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7])
    }

def choose_server(servers, need_c, need_m, need_d):
    # 1. Filter servers that can run the job
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # 2. Compute core gap
    for s in eligible:
        s["coreGap"] = s["cores"] - need_c

    # 3. Keep only those with smallest core gap
    eligible.sort(key=lambda x: x["coreGap"])
    best_gap = eligible[0]["coreGap"]
    gap_group = [s for s in eligible if s["coreGap"] == best_gap]

    # 4. Keep servers with highest memory in this group
    gap_group.sort(key=lambda x: x["memory"], reverse=True)
    max_mem = gap_group[0]["memory"]
    mem_group = [s for s in gap_group if s["memory"] == max_mem]

    # 5. Prioritise states: active > idle > booting > inactive
    state_priority = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}
    mem_group.sort(key=lambda x: state_priority.get(x["state"], 99))

    # 6. If still tied → pick fewest waiting jobs
    mem_group.sort(key=lambda x: x["waiting"])

    # Best server is now mem_group[0]
    return mem_group[0]


# ---------------------------------------------------------
# Main DS-Sim Client Logic (with TFPS)
# ---------------------------------------------------------

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

        # Simulator finishes
        if msg == "NONE":
            send(sock, "QUIT")
            receive(sock)
            break

        # Normal job event
        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

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

            # Choose server with TFPS
            selected = choose_server(servers, req_cores, req_mem, req_disk)

            if selected:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
                receive(sock)
            else:
                # fallback: just take first server in list
                f = servers[0]
                send(sock, f"SCHD {job_id} {f['type']} {f['id']}\n")
                receive(sock)

        # Protocol check handling
        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()