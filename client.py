#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

# ---------------------------------------------------------
# Basic network helpers (UNCHANGED PROTOCOL)
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
# Server parsing & selection (OPTIMISED)
# ---------------------------------------------------------

def parse_server(line):
    """
    Expected format from ds-server (brief mode):
      type id state curStartTime cores memory disk wJobs rJobs [cost ...]

    We keep everything you used before, and ALSO read rJobs.
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
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except ValueError:
            s["waiting"] = 0
            s["running"] = 0
    return s


def choose_server(servers, need_c, need_m, need_d, est_runtime):
    """
    Turnaround-focused heuristic (ECT-style):

    1. Filter servers that can run the job.
    2. For each eligible server, estimate a rough completion time:
         queue_size = waiting + running
         effective_cores = max(1, cores)
         ect = (queue_size * est_runtime) / effective_cores
    3. Sort by (ect, queue_size, -cores, -memory, waiting, id).

    Lower ect → we expect this job to finish sooner.
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

    candidates.sort(
        key=lambda pair: (
            pair[0],                     # estimated completion time
            pair[1]["waiting"] + pair[1]["running"],  # shorter queue
            -pair[1]["cores"],           # more cores
            -pair[1]["memory"],          # more memory
            pair[1]["waiting"],          # fewer waiting jobs
            pair[1]["id"],               # lower id
        )
    )
    return candidates[0][1]


# ---------------------------------------------------------
# Main DS-Sim Client Logic (protocol SAME, heuristic NEW)
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

            # Choose server with ECT-based heuristic
            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime)

            if selected is None:
                # fallback: just take first server in list
                f = servers[0]
                send(sock, f"SCHD {job_id} {f['type']} {f['id']}\n")
                receive(sock)
            else:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
                receive(sock)

        # Protocol check handling
        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            break

        # Completed job / other events → ignore and continue
        else:
            continue

    sock.close()


if __name__ == "__main__":
    main()
