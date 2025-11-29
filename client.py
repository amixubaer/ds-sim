#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

# ---------------------------------------------------------
# Basic network helpers
# ---------------------------------------------------------

def send(sock, msg: str) -> None:
    """Send a message (string) to the server."""
    sock.sendall(msg.encode("ascii"))


def receive(sock, timeout: float = 2.0) -> str:
    """
    Receive up to 8192 bytes, with a timeout.
    Returns a stripped ASCII string (may contain embedded newlines).
    """
    sock.settimeout(timeout)
    try:
        data = sock.recv(8192)
        if not data:
            return ""
        return data.decode("ascii", errors="ignore").strip()
    except Exception:
        return ""


# ---------------------------------------------------------
# Server parsing & selection
# ---------------------------------------------------------

def parse_server(line: str) -> dict:
    """
    Parse one server record line from GETS Capable.

    Expected format (assignment 2 variant):
      type id state curStartTime cores memory disk wJobs rJobs [cost ...]

    We only use a subset of fields plus wJobs.
    """
    parts = line.split()
    return {
        "type":   parts[0],
        "id":     int(parts[1]),
        "state":  parts[2],
        "cores":  int(parts[4]),
        "memory": int(parts[5]),
        "disk":   int(parts[6]),
        "waiting": int(parts[7]),
    }


def choose_server(servers, need_c, need_m, need_d):
    """
    Heuristic to improve turnaround time:

    1. Filter to servers that can run this job (cores/mem/disk).
    2. Among capable servers, prefer:
          - MORE cores (to finish jobs faster),
          - FEWER waiting jobs (to avoid long queues),
          - MORE memory as a gentle tie-breaker,
          - LOWER id as final tie-breaker (stable choice).
    """
    # 1. Filter capable servers
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # 2. Sort by our heuristic:
    #    - largest cores first  -> -cores
    #    - least waiting jobs   -> waiting
    #    - most memory          -> -memory
    #    - smaller id           -> id
    eligible.sort(
        key=lambda s: (-s["cores"], s["waiting"], -s["memory"], s["id"])
    )

    # Best server is the first one after sorting
    return eligible[0]


# ---------------------------------------------------------
# Main DS-Sim Client Logic
# ---------------------------------------------------------

def main():
    # Connect to ds-server
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except Exception:
        return

    # ===== Handshake =====
    send(sock, "HELO\n")
    if receive(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    if receive(sock) != "OK":
        sock.close()
        return

    # ===== Main event loop =====
    while True:
        send(sock, "REDY\n")
        msg = receive(sock)

        if not msg:
            break

        # Simulator is done
        if msg == "NONE":
            send(sock, "QUIT\n")
            receive(sock)
            break

        # Normal job event
        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()


            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            # est_runtime = int(parts[6])  # available but not strictly needed here

            # ---- Query capable servers ----
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}\n")
            header = receive(sock)

            if not header.startswith("DATA"):
                # If something weird happens, just continue to next REDY
                continue

            count = int(header.split()[1])

            # 1st OK to start receiving records
            send(sock, "OK\n")

            servers = []
            while len(servers) < count:
                chunk = receive(sock)
                if not chunk:
                    break
                for line in chunk.split("\n"):
                    line = line.strip()
                    if not line:
                        continue
                    servers.append(parse_server(line))
                    if len(servers) == count:
                        break

            # 2nd OK to signal we are done reading
            send(sock, "OK\n")

            # Read until we see "."
            while True:
                endmsg = receive(sock)
                if "." in endmsg:
                    break
                if not endmsg:
                    break

            # ---- Choose server using improved heuristic ----
            selected = choose_server(servers, req_cores, req_mem, req_disk)

            if selected is None:
                # Fallback: schedule on the first server if for some reason none were eligible
                selected = servers[0]

            send(
                sock,
                f"SCHD {job_id} {selected['type']} {selected['id']}\n"
            )
            receive(sock)  # expect "OK"

        # ds-server sometimes sends CHKQ for protocol checking
        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            break

    sock.close()


if __name__ == "__main__":
    main()
