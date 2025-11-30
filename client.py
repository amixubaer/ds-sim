#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"


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

    Generic format (this variant):
      type id state curStartTime cores mem disk wJobs [rJobs] [cost_or_rate ...]

    We will **only rely** on:
      - type, id, state, cores, mem, disk, wJobs
    and interpret an extra numeric field (if present) as a
    'speed indicator' (e.g. cost or rate).
    """
    parts = line.split()

    # Safe defaults
    s_type = parts[0]
    s_id = int(parts[1])
    state = parts[2].lower()
    cores = int(parts[4])
    mem = int(parts[5])
    disk = int(parts[6])
    waiting = int(parts[7]) if len(parts) > 7 else 0

    # Try to grab a "cost/rate" numeric value if present
    speed_metric = 1.0
    for p in parts[8:]:
        try:
            speed_metric = float(p)
            break
        except ValueError:
            continue

    return {
        "type": s_type,
        "id": s_id,
        "state": state,
        "cores": cores,
        "memory": mem,
        "disk": disk,
        "waiting": waiting,
        "speed_metric": speed_metric,
    }


def choose_server(servers, need_c, need_m, need_d):
    """
    Turnaround-focused heuristic:

    1. Filter by capacity (cores, memory, disk).
    2. Among eligible servers:
         - minimise queue length (waiting jobs)
         - prefer "faster" servers (higher speed_metric)
         - prefer more cores
         - prefer more memory
         - prefer smaller id (stable tie-break)

    state is used lightly to prefer active/idle over booting/inactive.
    """

    # Capacity filter
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # State priority: better state gets smaller number
    state_rank = {
        "active": 0,
        "idle": 1,
        "booting": 2,
        "inactive": 3,
    }

    def key(s):
        # Smaller queue better
        queue_len = s["waiting"]
        speed = s["speed_metric"]

        return (
            queue_len,                        # 1) fewer waiting jobs
            -speed,                           # 2) faster server (bigger metric)
            state_rank.get(s["state"], 4),    # 3) active/idle preferred
            -s["cores"],                      # 4) more cores
            -s["memory"],                     # 5) more memory
            s["id"],                          # 6) stable tie-break
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
            recv_line(sock)  # final response
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()

            # JOBN submitTime jobID cores memory disk estRuntime
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            # est_runtime = int(parts[6])  # not used directly here, but available

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

            # Choose server using aggressive TT heuristic
            selected = choose_server(servers, req_cores, req_mem, req_disk)
            if selected is None:
                # Fallback: first capable
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
            _ = recv_line(sock)  # expect "OK"

        elif msg.startswith("CHKQ"):
            # Protocol check event – respond and quit cleanly
            send(sock, "OK")
            _ = recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
