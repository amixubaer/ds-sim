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

    if a <= max_cores and b <= max_mem and c <= max_disk:
        cores, mem, disk, est_runtime = a, b, c, d
    else:
        est_runtime, cores, mem, disk = a, b, c, d

    return submit_time, job_id, est_runtime, cores, mem, disk


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


def choose_optimized(servers, est, cores, mem, disk):
    def score(s):
        w = s["wJobs"]
        r = s["rJobs"]

        ect = est * (1 + w + 0.5 * r)
        st_pen = state_priority(s["state"]) * est * 2
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
    parser.add_argument("--user", default="student")
    parser.add_argument("--debug", action="store_true")
    args = parser.parse_args()
    debug = args.debug

    # ===== Connect =====
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # optional banner
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ===== Handshake =====
    try:
        send_line(sock, "HELO")
        resp1 = recv_line(sock)

        send_line(sock, f"AUTH {args.user}")
        resp2 = recv_line(sock)
    except BrokenPipeError:
        print("Handshake failed: server closed connection early.", file=sys.stderr)
        sock.close()
        sys.exit(1)

    if debug:
        print("HANDSHAKE RESP1:", resp1, file=sys.stderr)
        print("HANDSHAKE RESP2:", resp2, file=sys.stderr)

    if resp1 != "OK" or resp2 != "OK":
        # IMPORTANT: don't send QUIT here; server already might have closed
        print("Handshake failed. Exiting.", file=sys.stderr)
        sock.close()
        sys.exit(1)

    # ===== bounds from GETS All =====
    all_servers = gets(sock, "GETS All")
    if not all_servers:
        sock.close()
        sys.exit(1)

    max_cores = max(s["cores"] for s in all_servers)
    max_mem = max(s["mem"] for s in all_servers)
    max_disk = max(s["disk"] for s in all_servers)

    # ===== Main loop =====
    while True:
        send_line(sock, "REDY")
        event = recv_line(sock)

        if debug:
            print("EVENT:", event, file=sys.stderr)

        if not event or event == "NONE":
            break

        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
            continue

        if event.startswith(("JOBN", "JOBP")):
            parts = event.split()
            if len(parts) < 7:
                continue

            submit, jid, est, cores, mem, disk = parse_job(
                parts, max_cores, max_mem, max_disk
            )

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
                print(f"SEND: SCHD {jid} {chosen['type']} {chosen['id']}", file=sys.stderr)

            send_line(sock, f"SCHD {jid} {chosen['type']} {chosen['id']}")
            recv_line(sock)

    # ===== Clean shutdown =====
    try:
        send_line(sock, "QUIT")
        recv_line(sock)
    except Exception:
        pass
    sock.close()


if __name__ == "__main__":
    main()
