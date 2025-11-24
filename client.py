#!/usr/bin/env python3
import socket
import sys
import argparse


def recv_line(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def send(sock, msg):
    sock.sendall((msg + "\n").encode())


# -------------------------
# Parse server record
# -------------------------
def parse_server(line):
    
    p = line.split()
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "start": int(p[3]),
        "cores": int(p[4]),
        "mem": int(p[5]),
        "disk": int(p[6]),
        "wJobs": int(p[7]) if len(p) > 7 else 0,
        "rJobs": int(p[8]) if len(p) > 8 else 0,
    }


# -------------------------
# GETS helper
# -------------------------
def gets(sock, cmd):
    send(sock, cmd)
    header = recv_line(sock)

    if not header.startswith("DATA"):
        return []

    n = int(header.split()[1])
    send(sock, "OK")

    servers = []
    for _ in range(n):
        line = recv_line(sock)
        if line:
            servers.append(parse_server(line))

    send(sock, "OK")
    _ = recv_line(sock)  # final "."
    return servers


# -------------------------
# State priority (cost + util aware)
# -------------------------
def state_priority(state: str) -> int:
    # lower is better (prefer already-on servers)
    if state == "idle":
        return 0
    if state == "active":
        return 1
    if state == "booting":
        return 2
    if state == "inactive":
        return 3
    return 4


# -------------------------
# Robust JOBN parser
# -------------------------
def parse_job(parts, max_cores, max_mem, max_disk):
    """
    Supports 2 JOBN formats:
    A) JOBN submit id est cores mem disk
    B) JOBN submit id cores mem disk est
    """
    jid = int(parts[2])

    a = int(parts[3])
    b = int(parts[4])
    c = int(parts[5])
    d = int(parts[6])

    # Detect if 'a' looks like cores
    if a <= max_cores and b <= max_mem and c <= max_disk:
        cores, mem, disk, est = a, b, c, d
    else:
        est, cores, mem, disk = a, b, c, d

    return jid, est, cores, mem, disk


# -------------------------
# Avail choice: Best Fit
# -------------------------
def choose_best_fit(servers, cores, mem, disk):
    return min(
        servers,
        key=lambda s: (
            (s["cores"] - cores),
            (s["mem"] - mem),
            (s["disk"] - disk),
            s["type"],
            s["id"],
        ),
    )


# -------------------------
# Capable choice: Optimized ECT + cost/util
# -------------------------
def choose_optimized(servers, est, cores, mem, disk):
    """
    Score components:
    1. ECT proxy:
       ect = est * (1 + wJobs + 0.5*rJobs)
    2. State penalty:
       booting/inactive cost more -> avoid unless needed
    3. Best-fit leftover:
       discourage wasting huge servers
    """
    def score(s):
        w = s["wJobs"]
        r = s["rJobs"]

        ect = est * (1 + w + 0.5 * r)

        # keep servers already running
        st_pen = state_priority(s["state"]) * est * 2

        # best-fit penalty (small)
        leftover = (s["cores"] - cores)
        fit_pen = leftover * 0.1 * est

        return (
            ect + st_pen + fit_pen,
            state_priority(s["state"]),
            leftover,
            s["type"],
            s["id"],
        )

    return min(servers, key=score)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", required=True)
    parser.add_argument("-p", "--port", type=int, default=50000)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))


    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ---- Handshake ----
    send(sock, "HELO")
    recv_line(sock)

    send(sock, "AUTH AbuJubaer")   
    recv_line(sock)

    # ---- Static server list to get bounds ----
    all_servers = gets(sock, "GETS All")
    if not all_servers:
        send(sock, "QUIT")
        sock.close()
        return

    max_cores = max(s["cores"] for s in all_servers)
    max_mem = max(s["mem"] for s in all_servers)
    max_disk = max(s["disk"] for s in all_servers)

    # ---- Main loop ----
    while True:
        send(sock, "REDY")
        event = recv_line(sock)

        if not event or event == "NONE":
            break

        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
            continue

        if event.startswith(("JOBN", "JOBP")):
            parts = event.split()
            if len(parts) < 7:
                continue

            jid, est, cores, mem, disk = parse_job(
                parts, max_cores, max_mem, max_disk
            )

            # 1) Try Avail first
            avail = gets(sock, f"GETS Avail {cores} {mem} {disk}")
            if avail:
                chosen = choose_best_fit(avail, cores, mem, disk)
            else:
                capable = gets(sock, f"GETS Capable {cores} {mem} {disk}")
                if capable:
                    chosen = choose_optimized(capable, est, cores, mem, disk)
                else:
                    # ultra-safe fallback
                    chosen = choose_best_fit(all_servers, cores, mem, disk)

            send(sock, f"SCHD {jid} {chosen['type']} {chosen['id']}")
            recv_line(sock)

    send(sock, "QUIT")
    try:
        recv_line(sock)
    except Exception:
        pass
    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
