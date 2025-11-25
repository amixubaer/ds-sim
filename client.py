#!/usr/bin/env python3

import socket
import sys
import argparse


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
# DS-Sim helpers
# -------------------------
def parse_server(line):
    # type id state curStartTime cores mem disk wJobs rJobs
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
    """Send GETS command and parse the DATA block."""
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


def parse_job(parts, max_cores, max_mem, max_disk):
    """
    Supports BOTH JOBN formats:
      A) JOBN submit id est cores mem disk
      B) JOBN submit id cores mem disk est
    """
    submit_time = int(parts[1])
    job_id = int(parts[2])

    a = int(parts[3])
    b = int(parts[4])
    c = int(parts[5])
    d = int(parts[6])

    # If a looks like cores and b/c fit mem/disk bounds => format B
    if a <= max_cores and b <= max_mem and c <= max_disk:
        cores, mem, disk, est_runtime = a, b, c, d
    else:
        est_runtime, cores, mem, disk = a, b, c, d

    return submit_time, job_id, est_runtime, cores, mem, disk


def choose_best_fit(servers, cores, mem, disk):
    """Best-Fit among available servers."""
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


def choose_optimized(servers, est, cores, mem, disk):
    """
    Optimized ECT hybrid:
    - estimated completion time proxy using queue
    - prefer active/idle servers (lower cost)
    - best-fit tie-break to avoid wasting big servers
    """
    def score(s):
        w = s["wJobs"]
        r = s["rJobs"]

        # estimated completion time proxy
        ect = est * (1 + w + 0.5 * r)

        # penalty for turning on new servers
        st_pen = state_priority(s["state"]) * est * 2

        # mild best-fit penalty
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
    parser.add_argument("--algo", required=True, help="Algo name (only for your tracking).")
    parser.add_argument("-p", "--port", type=int, default=50000)
    parser.add_argument("--user", default="student", help="Username for AUTH.")
    parser.add_argument("--debug", action="store_true", help="Print JOBN / SCHD events.")
    args = parser.parse_args()

    debug = args.debug

    # ===== Connect to server =====
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # optional initial banner (when ds-server not using -n)
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ===== Handshake =====
    send_line(sock, "HELO")
    resp1 = recv_line(sock)

    send_line(sock, f"AUTH {args.user}")  # MUST be username
    resp2 = recv_line(sock)

    if resp1 != "OK" or resp2 != "OK":
        send_line(sock, "QUIT")
        try:
            recv_line(sock)
        except Exception:
            pass
        sock.close()
        sys.exit(1)

    # ===== GETS All (bounds only) =====
    all_servers = gets(sock, "GETS All")
    if not all_servers:
        send_line(sock, "QUIT")
        sock.close()
        sys.exit(1)

    max_cores = max(s["cores"] for s in all_servers)
    max_mem = max(s["mem"] for s in all_servers)
    max_disk = max(s["disk"] for s in all_servers)

    # ===== Main scheduling loop =====
    while True:
        send_line(sock, "REDY")
        event = recv_line(sock)

        if debug:
            print("EVENT:", event, file=sys.stderr)

        if not event or event == "NONE":
            break

        # Ignore completion / resource messages
        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
            continue

        if event.startswith(("JOBN", "JOBP")):
            parts = event.split()
            if len(parts) < 7:
                continue

            submit_time, job_id, est, cores, mem, disk = parse_job(
                parts, max_cores, max_mem, max_disk
            )

            # 1) Avail first
            avail = gets(sock, f"GETS Avail {cores} {mem} {disk}")
            if avail:
                chosen = choose_best_fit(avail, cores, mem, disk)
            else:
                capable = gets(sock, f"GETS Capable {cores} {mem} {disk}")
                if capable:
                    chosen = choose_optimized(capable, est, cores, mem, disk)
                else:
                    chosen = choose_best_fit(all_servers, cores, mem, disk)

            if debug:
                print(
                    f"SEND: SCHD {job_id} {chosen['type']} {chosen['id']}",
                    file=sys.stderr
                )

            send_line(sock, f"SCHD {job_id} {chosen['type']} {chosen['id']}")
            _ = recv_line(sock)  # expect OK

    # ===== Clean shutdown =====
    send_line(sock, "QUIT")
    try:
        recv_line(sock)
    except Exception:
        pass

    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
