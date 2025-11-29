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
# Server parsing & selection
# ---------------------------------------------------------

def parse_server(line):
    parts = line.split()
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        # parts[3] = curStartTime (ignored)
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7]) if len(parts) > 7 else 0
        # rJobs may exist at parts[8], we ignore for simplicity
    }

def fetch_servers(sock, cmd):
    """
    Generic GETS helper that uses your existing recv style,
    but works for both GETS Avail and GETS Capable.
    """
    send(sock, cmd + "\n")
    header = receive(sock)

    if not header or not header.startswith("DATA"):
        return []

    count = int(header.split()[1])

    # First OK
    send(sock, "OK\n")

    servers = []
    while len(servers) < count:
        chunk = receive(sock)
        if not chunk:
            continue
        for line in chunk.split("\n"):
            line = line.strip()
            if line:
                servers.append(parse_server(line))
                if len(servers) == count:
                    break

    # Second OK
    send(sock, "OK\n")

    # Consume final "."
    while True:
        endmsg = receive(sock)
        if "." in endmsg:
            break

    return servers

def choose_server(servers, need_c, need_m, need_d, est_runtime):
    """
    Queue-aware, best-fit heuristic:

    - Only servers that can run the job are considered.
    - Score = (waiting+1)*est_runtime   (ECT-like)
      then tie-break by:
        * state: active < idle < booting < inactive
        * core gap (smaller is better)
        * more memory
        * lower id
    """
    # Filter eligible
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    state_priority = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}

    def score(s):
        core_gap = s["cores"] - need_c
        return (
            (s["waiting"] + 1) * est_runtime,             # queue * runtime (ECT proxy)
            state_priority.get(s["state"], 99),           # prefer active/idle
            core_gap,                                     # best fit in cores
            -s["memory"],                                 # more memory is better
            s["id"],                                      # stable tiebreak
        )

    return min(eligible, key=score)


# ---------------------------------------------------------
# Main DS-Sim Client Logic (Optimised, still safe)
# ---------------------------------------------------------

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except:
        return

    # Handshake (unchanged behaviour)
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
            send(sock, "QUIT\n")
            receive(sock)
            break

        # Normal job event
        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            # ⚠️ KEEP this exactly as in your working version.
            # Your ds-server build clearly expects this position for job_id.
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            # New: use estimated runtime if present
            est_runtime = int(parts[6]) if len(parts) > 6 else 1

            # First try GETS Avail (fastest turnaround)
            servers = fetch_servers(sock, f"GETS Avail {req_cores} {req_mem} {req_disk}")

            # If no immediately available server, fall back to GETS Capable
            if not servers:
                servers = fetch_servers(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")

            if not servers:
                # Should almost never happen; just REDY again
                continue

            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime)

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
            send(sock, "QUIT\n")
            break

    sock.close()


if __name__ == "__main__":
    main()
