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


def send_line(sock, msg: str):
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


def choose_best_fit(servers, cores, mem, disk):
    return min(
        servers,
        key=lambda s: (
            s["cores"] - cores,
            s["mem"] - mem,
            s["disk"] - disk,
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
        leftover = s["cores"] - cores
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

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # optional banner
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    sock.settimeout(None)

    # ---- Handshake ----
    send_line(sock, "HELO")
    resp1 = recv_line(sock)
    send_line(sock, f"AUTH {args.user}")
    resp2 = recv_line(sock)

    if resp1 != "OK" or resp2 != "OK":
        sock.close()
        sys.exit(1)

    # ---- Static list for fallback + bounds ----
    all_servers = gets(sock, "GETS All")
    if not all_servers:
        sock.close()
        sys.exit(1)

    max_cores = max(s["cores"] for s in all_servers)
    max_mem = max(s["mem"] for s in all_servers)
    max_disk = max(s["disk"] for s in all_servers)

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

            # YOUR server format confirmed:
            # JOBN submit jobID cores mem disk est
            submit_time = int(parts[1])
            job_id = int(parts[2])
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # Try Avail, else Capable
            avail = gets(sock, f"GETS Avail {cores} {mem} {disk}")
            if avail:
                candidates = avail
                chosen = choose_best_fit(candidates, cores, mem, disk)
            else:
                candidates = gets(sock, f"GETS Capable {cores} {mem} {disk}")
                if candidates:
                    chosen = choose_optimized(candidates, est_runtime, cores, mem, disk)
                else:
                    candidates = all_servers
                    chosen = choose_best_fit(candidates, cores, mem, disk)

            # ---- SCHD + check reply ----
            def try_schd(server):
                send_line(sock, f"SCHD {job_id} {server['type']} {server['id']}")
                return recv_line(sock)

            reply = try_schd(chosen)

            if debug:
                print(f"SEND: SCHD {job_id} {chosen['type']} {chosen['id']} -> {reply}",
                      file=sys.stderr)

            # If server rejects, try next best capable server
            if reply != "OK":
                # remove rejected one and retry from capable list
                fallback_list = candidates[:]
                fallback_list = [s for s in fallback_list
                                 if not (s["type"] == chosen["type"] and s["id"] == chosen["id"])]

                if not fallback_list:
                    fallback_list = gets(sock, f"GETS Capable {cores} {mem} {disk}")

                if fallback_list:
                    alt = choose_best_fit(fallback_list, cores, mem, disk)
                    reply2 = try_schd(alt)
                    if debug:
                        print(f"RETRY: SCHD {job_id} {alt['type']} {alt['id']} -> {reply2}",
                              file=sys.stderr)

    # ---- shutdown ----
    try:
        send_line(sock, "QUIT")
        recv_line(sock)
    except Exception:
        pass
    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
