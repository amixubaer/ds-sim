#!/usr/bin/env python3

import socket
import sys
import argparse


# -------------------------
# I/O helpers
# -------------------------
def recv_line(sock):
    """Receive a single newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def send_line(sock, msg: str):
    sock.sendall((msg + "\n").encode())


# -------------------------
# ds-sim helpers
# -------------------------
def parse_server(line):
    """
    Server record format (MQ ds-server):
    type id state curStartTime cores mem disk wJobs rJobs
    """
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


def gets(sock, cmd):
    """Send GETS command and parse returned server list."""
    send_line(sock, cmd)
    header = recv_line(sock)

    if not header.startswith("DATA"):
        return []

    n = int(header.split()[1])
    send_line(sock, "OK")

    servers = []
    for _ in range(n):
        line = recv_line(sock)
        if line:
            servers.append(parse_server(line))

    send_line(sock, "OK")
    _ = recv_line(sock)  # final "."
    return servers


def state_priority(state: str) -> int:
    """
    Prefer servers already on:
      idle < active < booting < inactive
    """
    if state == "idle":
        return 0
    if state == "active":
        return 1
    if state == "booting":
        return 2
    if state == "inactive":
        return 3
    return 4


def best_fit(servers, cores, mem, disk):
    """Best-fit to reduce waste (fast tie-break)."""
    return min(
        servers,
        key=lambda s: (
            s["cores"] - cores,
            s["mem"] - mem,
            s["disk"] - disk,
            state_priority(s["state"]),
            s["type"],
            s["id"],
        ),
    )


def choose_fast_ect(servers, est_runtime, cores, mem, disk):
    """
    Fast ECT heuristic:
    - uses wJobs/rJobs as queue proxy
    - prefers idle/active servers
    - best-fit as tie-break
    """
    def score(s):
        w = s["wJobs"]
        r = s["rJobs"]

        # queue-aware estimated completion proxy
        ect = est_runtime * (1 + w + 0.5 * r)

        # avoid booting/inactive unless needed
        st_pen = state_priority(s["state"]) * est_runtime * 2

        # mild best-fit penalty
        leftover = s["cores"] - cores
        fit_pen = leftover * 0.1 * est_runtime

        return (
            ect + st_pen + fit_pen,
            state_priority(s["state"]),
            leftover,
            s["type"],
            s["id"],
        )

    return min(servers, key=score)


# -------------------------
# Main
# -------------------------
def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", required=True, help="Sent in AUTH (keep as in your working code).")
    parser.add_argument("-p", "--port", type=int, default=50000)
    args = parser.parse_args()

    # ----- Connect -----
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # optional greeting
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ----- Handshake (same as your working version) -----
    send_line(sock, "HELO")
    resp1 = recv_line(sock)

    send_line(sock, f"AUTH {args.algo}")
    resp2 = recv_line(sock)

    if resp1 != "OK" or resp2 != "OK":
        try:
            send_line(sock, "QUIT")
            recv_line(sock)
        except Exception:
            pass
        sock.close()
        sys.exit(1)

    # Cache static list once (for bounds + fallback)
    all_servers = gets(sock, "GETS All")
    if not all_servers:
        send_line(sock, "QUIT")
        sock.close()
        sys.exit(1)

    # ----- Scheduling loop -----
    while True:
        send_line(sock, "REDY")
        event = recv_line(sock)

        if not event or event == "NONE":
            break

        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
            continue

        if event.startswith(("JOBN", "JOBP")):
            parts = event.split()
            if len(parts) < 7:
                continue

            # Your confirmed MQ format:
            # JOBN submitTime jobID cores mem disk estRuntime
            submit_time = int(parts[1])       # not used, but correct
            job_id_a = int(parts[2])          # standard MQ jobID
            job_id_b = int(parts[1])          # fallback only if server complains
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # 1) Avail-first (fastest turnaround)
            avail = gets(sock, f"GETS Avail {cores} {mem} {disk}")
            if avail:
                chosen = best_fit(avail, cores, mem, disk)
            else:
                capable = gets(sock, f"GETS Capable {cores} {mem} {disk}")
                if capable:
                    chosen = choose_fast_ect(capable, est_runtime, cores, mem, disk)
                else:
                    chosen = best_fit(all_servers, cores, mem, disk)

            # Send SCHD, check reply
            def schd(jid):
                send_line(sock, f"SCHD {jid} {chosen['type']} {chosen['id']}")
                return recv_line(sock)

            reply = schd(job_id_a)

            # Rare safety: if jobID position mismatch, retry once
            if "No such waiting job exists" in reply:
                reply2 = schd(job_id_b)
                # if still not OK, just continue and server will resend
                if reply2 != "OK":
                    continue

    # ----- Clean shutdown -----
    try:
        send_line(sock, "QUIT")
        recv_line(sock)
    except Exception:
        pass

    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
