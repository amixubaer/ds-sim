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


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--algo",
        required=True,
        help="Scheduling algorithm name (sent in AUTH only).",
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

    # Try to read one line with a short timeout; if nothing, ignore.
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except (socket.timeout, OSError):
        pass
    finally:
        sock.settimeout(None)

    # ===== Handshake =====
    sock.sendall(b"HELO\n")
    resp1 = recv_line(sock)

    # We still send the algo string in AUTH, as before
    sock.sendall(f"AUTH {args.algo}\n".encode())
    resp2 = recv_line(sock)

    if resp1 != "OK" or resp2 != "OK":
        sock.sendall(b"QUIT\n")
        try:
            recv_line(sock)
        except Exception:
            pass
        sock.close()
        sys.exit(1)

    # ===== GETS All: get static server list =====
    sock.sendall(b"GETS All\n")
    data_resp = recv_line(sock)

    parts = data_resp.split()
    if len(parts) < 3 or not data_resp.startswith("DATA"):
        sock.sendall(b"QUIT\n")
        try:
            recv_line(sock)
        except Exception:
            pass
        sock.close()
        sys.exit(1)

    n_recs = int(parts[1])

    # Acknowledge DATA
    sock.sendall(b"OK\n")

    # Server state: capacity, cost, and estimated finish_time
    # Record format (this variant): type id state curStartTime cores mem disk [wJobs rJobs cost ...]
    server_state = {}
    max_cost = 0.0

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()
        s_type = rec[0]
        s_id = rec[1]
        # state     = rec[2]  # not needed for our heuristic
        cur_start = int(rec[3])
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        # Cost may not be present in all builds; default to 1.0 if missing/invalid
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
            "cost": cost,
            # Estimated time when this server becomes free.
            # Initialise with cur_start to include any initial load.
            "finish_time": cur_start,
        }
        if cost > max_cost:
            max_cost = cost

    # Finish GETS All sequence
    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    if max_cost <= 0:
        max_cost = 1.0

    # Approximate global time based on job submissions / completions
    current_time = 0

    # ===== Main scheduling loop: ECT-style heuristic =====
    while True:
        sock.sendall(b"REDY\n")
        event = recv_line(sock)

        if not event:
            break

        if event == "NONE":
            break

        parts = event.split()

        # Track time hints from JCPL if available
        if event.startswith("JCPL"):
            # JCPL time serverType serverID jobID
            if len(parts) >= 5:
                t = int(parts[1])
                s_type = parts[2]
                s_id = parts[3]
                current_time = max(current_time, t)
                key = (s_type, s_id)
                if key in server_state:
                    # Make sure finish_time is at least this completion time
                    s = server_state[key]
                    s["finish_time"] = max(s["finish_time"], t)
            continue

        # Ignore other non-job events
        if event.startswith(("RESF", "RESR", "CHKQ")):
            continue

        # New or pre-empted job
        if event.startswith("JOBN") or event.startswith("JOBP"):
            # In this ds-sim variant: JOBN submitTime jobID cores memory disk estRuntime
            if len(parts) < 7:
                continue

            submit_time = int(parts[1])
            job_id = parts[2]
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # Update approx current_time with submit time
            current_time = max(current_time, submit_time)
            rt = max(est_runtime, 1)

            # ===== ECT: choose server with earliest estimated completion =====
            best_key = None
            best_score = None

            for key, s in server_state.items():
                if (
                    s["cores"] >= cores
                    and s["mem"] >= mem
                    and s["disk"] >= disk
                ):
                    # When could this job start on this server?
                    start_time = max(s["finish_time"], submit_time, current_time)
                    completion_time = start_time + rt

                    # Primary metric: completion time
                    score = float(completion_time)

                    # Tiny cost tie-breaker: prefer cheaper servers a bit
                    score += 0.001 * s["cost"] * rt

                    if best_score is None or score < best_score:
                        best_score = score
                        best_key = key

            # Fallback: if somehow no capable server found, just use the first one
            if best_key is None:
                if not server_state:
                    sock.sendall(b"QUIT\n")
                    try:
                        recv_line(sock)
                    except Exception:
                        pass
                    sock.close()
                    sys.exit(1)
                best_key = next(iter(server_state.keys()))

            s_type, s_id = best_key
            s_info = server_state[best_key]

            # Update that server's estimated finish time
            start_time = max(s_info["finish_time"], submit_time, current_time)
            completion_time = start_time + rt
            s_info["finish_time"] = completion_time

            # Send schedule command
            cmd = f"SCHD {job_id} {s_type} {s_id}\n"
            sock.sendall(cmd.encode())
            _ = recv_line(sock)  # expect "OK"

            continue

        # Any other event types are ignored

    # ===== Clean shutdown =====
    sock.sendall(b"QUIT\n")
    try:
        _ = recv_line(sock)
    except Exception:
        pass

    sock.close()
    sys.exit(0)


if __name__ == "__main__":
    main()
