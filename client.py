#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

# ---------------------------------------------------------
# Basic network helpers (UNCHANGED PROTOCOL)
# ---------------------------------------------------------

def send(sock, msg):
    # You originally always included the '\n' in msg
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
    Turnaround-focused heuristic (ECT-style), now state-aware:

    1. Filter servers that can run the job (capacity constraint).
    2. For each eligible server, compute a rough Estimated Completion Time (ECT):
         queue_size = waiting + running
         effective_cores = max(1, cores)
         base_ect = (queue_size + 1) * est_runtime / effective_cores
         state penalty: active < idle < booting << inactive
    3. Pick the server with minimal (ECT * state_penalty), tie-breaking by:
         - smaller queue_size
         - more cores
         - more memory
         - smaller id
    """

    # Capacity filter
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # State weights: penalise non-active servers to improve turnaround
    state_weight = {
        "active": 1.0,
        "idle": 1.2,
        "booting": 2.5,
        "inactive": 50.0,   # basically avoid unless nothing else exists
    }

    candidates = []
    for s in eligible:
        queue_size = s["waiting"] + s["running"]
        eff_cores = max(1, s["cores"])

        # base ECT: more jobs and fewer cores -> larger time
        base_ect = (queue_size + 1) * max(est_runtime, 1) / eff_cores

        # state-aware penalty
        penalty = state_weight.get(s["state"].lower(), 3.0)
        ect = base_ect * penalty

        candidates.append((ect, queue_size, s))

    # Sort by ECT, then queue length, then cores/mem/id
    candidates.sort(
        key=lambda x: (
            x[0],                        # ECT (lower is better)
            x[1],                        # smaller queue
            -x[2]["cores"],              # more cores
            -x[2]["memory"],             # more memory
            x[2]["id"],                  # lower id
        )
    )

    # Best candidate server
    return candidates[0][2]



# ---------------------------------------------------------
# Main DS-Sim Client Logic (protocol SAME, heuristic NEW)
# ---------------------------------------------------------

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except:
        return

    # Handshake (exactly as in your working version)
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
            # JOBN submitTime jobID cores memory disk estRuntime
            #         0         1     2     3      4    5      6
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
