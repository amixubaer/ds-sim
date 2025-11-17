#!/usr/bin/env python3

import socket
import sys
import argparse
import math


def recv_line(sock):
    """Receive a single newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--algo",
        required=True,
        help="Scheduling algorithm name (sent in AUTH).",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=50000,
        help="Port ds-server is listening on (default 50000).",
    )
    args = parser.parse_args()

    # ===== Connect to server =====
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # Optional: try to read any initial greeting (if server sends one)
    try:
        sock.settimeout(0.5)
        _ = recv_line(sock)  # ignore contents
    except socket.timeout:
        pass
    finally:
        sock.settimeout(None)

    # ===== Handshake =====
    sock.sendall(b"HELO\n")
    resp1 = recv_line(sock)

    sock.sendall(f"AUTH {args.algo}\n".encode())
    resp2 = recv_line(sock)

    if resp1 != "OK" or resp2 != "OK":
        sock.close()
        sys.exit(1)

    # ===== GETS All: get static server list =====
    sock.sendall(b"GETS All\n")
    data_resp = recv_line(sock)

    parts = data_resp.split()
    if len(parts) < 3 or not data_resp.startswith("DATA"):
        sock.sendall(b"QUIT\n")
        recv_line(sock)
        sock.close()
        sys.exit(1)

    n_recs = int(parts[1])

    sock.sendall(b"OK\n")

    # Server state: maintain predicted availability for ECT
    # Record format: type id state curStartTime cores mem disk wJobs rJobs cost ...
    server_state = {}
    max_cost = 0.0

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()
        s_type = rec[0]
        s_id = rec[1]
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        cost = 1.0
        if len(rec) >= 9:
            try:
                cost = float(rec[8])
            except ValueError:
                cost = 1.0

        server_state[(s_type, s_id)] = {
            "cores": cores,
            "mem": mem,
            "disk": disk,
            "available_at": 0,  # our predicted next free time
            "cost": cost,
        }
        if cost > max_cost:
            max_cost = cost

    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    if max_cost <= 0:
        max_cost = 1.0

    # ===== Main scheduling loop: Earliest Completion Time (ECT) =====
    while True:
        sock.sendall(b"REDY\n")
        event = recv_line(sock)

        if not event:
            break

        if event == "NONE":
            break

        parts = event.split()

        # Job completion: update our predicted availability
        if event.startswith("JCPL"):
            if len(parts) >= 5:
                finish_time = int(parts[1])
                s_type = parts[3]
                s_id = parts[4]
                key = (s_type, s_id)
                if key in server_state and finish_time > server_state[key]["available_at"]:
                    server_state[key]["available_at"] = finish_time
            continue

        # Ignore other non-job events
        if event.startswith(("RESF", "RESR", "CHKQ")):
            continue

        # New or pre-empted job
        if event.startswith("JOBN") or event.startswith("JOBP"):
            # MQ spec: JOBN submitTime jobID cores memory disk estRuntime
            if len(parts) < 7:
                continue

            submit_time = int(parts[1])
            job_id = parts[2]
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            best_server = None
            best_finish = math.inf

            # Earliest Completion Time: min over capable servers
            for key, s in server_state.items():
                if s["cores"] >= cores and s["mem"] >= mem and s["disk"] >= disk:
                    start_time = max(s["available_at"], submit_time)
                    finish_time = start_time + est_runtime
                    if finish_time < best_finish:
                        best_finish = finish_time
                        best_server = key

            if best_server is None:
                # Fallback: first server if something weird happens
                if not server_state:
                    sock.sendall(b"QUIT\n")
                    recv_line(sock)
                    sock.close()
                    sys.exit(1)
                best_server = next(iter(server_state.keys()))
                best_finish = submit_time + est_runtime

            s_type, s_id = best_server
            cmd = f"SCHD {job_id} {s_type} {s_id}\n"
            sock.sendall(cmd.encode())
            _ = recv_line(sock)  # expect "OK"

            # Update predicted availability
            server_state[best_server]["available_at"] = best_finish

            continue

        # Anything else: ignore

    # ===== Clean shutdown =====
    sock.sendall(b"QUIT\n")
    _ = recv_line(sock)
    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
