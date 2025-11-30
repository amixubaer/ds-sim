#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"


# -----------------------------
# Basic send/recv helpers
# -----------------------------
def send(sock, msg: str) -> None:
    """Send a newline-terminated ASCII message."""
    sock.sendall((msg + "\n").encode("ascii"))


def recv_line(sock) -> str:
    """Receive one newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode("ascii", errors="ignore").strip()


# -----------------------------
# Server parsing & selection
# -----------------------------
def parse_server(line: str):
    """
    Parse one server record line from GETS Capable / GETS All.

    Expected format:
      type id state curStartTime cores memory disk wJobs rJobs [cost ...]

    We only need: type, id, state, cores, memory, disk, wJobs, rJobs.
    """
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
        # indexes 7, 8 are wJobs, rJobs per ds-sim spec
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except ValueError:
            s["waiting"] = 0
            s["running"] = 0
    return s


def choose_server(servers, need_c, need_m, need_d, est_runtime):
    """
    Turnaround-focused heuristic:

    - Only consider servers that can run the job.
    - For each candidate, estimate a rough completion time:

        queue_size = waiting + running
        effective_cores = max(1, cores)
        ect = (queue_size * est_runtime) / effective_cores

      → fewer queued jobs, more cores, and shorter runtime ⇒ smaller ect.

    - Tie-breakers:
        1. smaller ect
        2. more cores
        3. more memory
        4. fewer waiting jobs
        5. lower id
    """
    candidates = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            queue_size = s["waiting"] + s["running"]
            eff_cores = max(1, s["cores"])
            ect = (queue_size * max(est_runtime, 1)) / eff_cores
            candidates.append((ect, s))

    if not candidates:
        return None

    # Sort by ECT, then tie-breakers
    candidates.sort(
        key=lambda pair: (
            pair[0],                    # estimated completion time
            -pair[1]["cores"],          # prefer more cores
            -pair[1]["memory"],         # then more memory
            pair[1]["waiting"],         # then fewer waiting jobs
            pair[1]["id"],              # then lower id
        )
    )
    return candidates[0][1]


# -----------------------------
# Main client
# -----------------------------
def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # ---- Handshake ----
    send(sock, "HELO\n")
    resp = recv_line(sock)
    if resp != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    resp = recv_line(sock)
    if resp != "OK":
        sock.close()
        return

    # ---- Main event loop ----
    while True:
        send(sock, "REDY\n")
        msg = recv_line(sock)

        if not msg:
            break

        # Simulation finished
        if msg == "NONE":
            send(sock, "QUIT\n")
            recv_line(sock)  # final OK
            break

        # Normal job event
        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
           
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            # Ask for capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}\n")
            header = recv_line(sock)

            if not header.startswith("DATA"):
                # protocol error – be safe and QUIT
                send(sock, "QUIT\n")
                recv_line(sock)
                break

            n_recs = int(header.split()[1])

            # Acknowledge header
            send(sock, "OK\n")

            servers = []
            for _ in range(n_recs):
                line = recv_line(sock)
                if line:
                    servers.append(parse_server(line))

            # Finish GETS
            send(sock, "OK\n")
            end = recv_line(sock)  # should be "."

            # Select server using ECT heuristic
            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime)
            if selected is None:
                # Fallback: first server in list
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
            _ = recv_line(sock)  # expect "OK"

        # Protocol check from server
        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            recv_line(sock)  # server responds
            send(sock, "QUIT\n")
            recv_line(sock)
            break

        else:

            continue

    sock.close()


if __name__ == "__main__":
    main()
