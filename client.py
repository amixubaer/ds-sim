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
    send(sock, cmd)
    header = recv_line(sock)

    if not header.startswith("DATA"):
        return []

    n = int(header.split()[1])
    send(sock, "OK")

    servers = []
    for _ in range(n):
        servers.append(parse_server(recv_line(sock)))

    send(sock, "OK")
    _ = recv_line(sock)  # final "."
    return servers


def state_priority(state: str) -> int:
    # lower is better
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
    # smallest leftover resources among candidates
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


def best_ect(servers, est_runtime, cores, mem, disk):
    # estimated completion time proxy
    def ect_score(s):
        queue = s["wJobs"] + s["rJobs"]
        ect = queue * est_runtime + est_runtime
        fit = (s["cores"] - cores)
        return (ect, state_priority(s["state"]), fit, s["type"], s["id"])

    return min(servers, key=ect_score)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--algo", required=True)
    parser.add_argument("-p", "--port", type=int, default=50000)
    args = parser.parse_args()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # optional initial line (when ds-server not using -n)
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ---- Handshake ----
    send(sock, "HELO")
    recv_line(sock)

    # AUTH must be username, NOT algo
    send(sock, "AUTH AbuJubaer")
    recv_line(sock)

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

            # correct JOBN format:
            # JOBN submitTime jobID estRuntime cores mem disk
            job_id = int(parts[2])
            est_runtime = int(parts[3])
            cores = int(parts[4])
            mem = int(parts[5])
            disk = int(parts[6])

            # 1) Avail first
            avail = gets(sock, f"GETS Avail {cores} {mem} {disk}")
            if avail:
                chosen = best_fit(avail, cores, mem, disk)
            else:
                capable = gets(sock, f"GETS Capable {cores} {mem} {disk}")
                chosen = best_ect(capable, est_runtime, cores, mem, disk)

            send(sock, f"SCHD {job_id} {chosen['type']} {chosen['id']}")
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
