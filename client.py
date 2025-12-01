#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"


# ---------------------------------------------------------
# Basic helpers
# ---------------------------------------------------------

def send(sock, msg: str) -> None:
    """Send a line (with newline) to the server."""
    sock.sendall((msg + "\n").encode())


def recv_line(sock) -> str:
    """Receive a single newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


# ---------------------------------------------------------
# Server parsing & selection
# ---------------------------------------------------------

def parse_server(line: str) -> dict:
    """
    Parse a server record line from GETS Capable / GETS All.

    Expected format (per ds-sim brief variant):
      type id state curStartTime cores mem disk wJobs rJobs rate relTime

    We use: type, id, state, cores, mem, disk, wJobs, rJobs.
    """
    parts = line.split()
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2].lower(),
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7]),
        "running": int(parts[8]) if len(parts) > 8 else 0,
    }


def choose_server(servers, need_c, need_m, need_d, est_runtime):
    """
    Turnaround-focused heuristic:

    1. Filter servers that can run the job (capacity).
    2. Among eligible servers, minimise total queue length (waiting+running).
    3. Tie-break by:
         - better state (active < idle < booting < inactive)
         - more cores
         - more memory
         - smaller id
    """

    # 1. Capacity filter
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # 2. State ranking (better -> smaller number)
    state_rank = {
        "active": 0,
        "idle": 1,
        "booting": 2,
        "inactive": 3,
    }

    # 3. Sort by queue length etc.
    def key(s):
        queue = s["waiting"] + s["running"]
        return (
            queue,                              # smaller queue first
            state_rank.get(s["state"], 4),      # better state
            -s["cores"],                        # more cores
            -s["memory"],                       # more memory
            s["id"],                            # smaller id
        )

    return min(eligible, key=key)


# ---------------------------------------------------------
# Main DS-Sim client
# ---------------------------------------------------------

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # ===== Handshake =====
    send(sock, "HELO")
    _ = recv_line(sock)  # expect "OK"

    send(sock, f"AUTH {USER}")
    _ = recv_line(sock)  # expect "OK"

    # ===== Main loop =====
    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)  
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()

          
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            # Request capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")
            header = recv_line(sock)

            if not header.startswith("DATA"):
                continue

            n_recs = int(header.split()[1])

            # First OK
            send(sock, "OK")

            # Read server records
            servers = []
            for _ in range(n_recs):
                line = recv_line(sock)
                servers.append(parse_server(line))

            # Second OK
            send(sock, "OK")

            # Read final "."
            while recv_line(sock) != ".":
                pass

            # Choose server with turnaround-focused heuristic
            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime)
            if selected is None:
                # Fallback: just use first server
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
            _ = recv_line(sock)  # expect "OK"

        elif msg.startswith("CHKQ"):
            # Protocol-check event – respond and quit
            send(sock, "OK")
            _ = recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
