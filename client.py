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
        help="Scheduling algorithm name (used locally + sent in AUTH).",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int,
        default=50000,
        help="Port ds-server is listening on (default 50000).",
    )
    args = parser.parse_args()
    algo = args.algo.lower()

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

    # We send the algo string in AUTH (as you were already doing)
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

    # Server state: capacity, cost, and an estimated finish_time
    # Record format (this variant): type id state curStartTime cores mem disk [wJobs rJobs cost ...]
    server_state = {}
    max_cost = 0.0

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()
        s_type = rec[0]
        s_id = rec[1]
        # state      = rec[2]  # not used directly
        cur_start = int(rec[3])  # when this server next starts its current job load
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
            # We initialise with cur_start to reflect any initial workload.
            "finish_time": cur_start,
        }
        if cost > max_cost:
            max_cost = cost

    # Finish GETS All sequence
    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    if max_cost <= 0:
        max_cost = 1.0

    # ===== Helper: choose server by algorithm =====
    def choose_server_fc(job_cores, job_mem, job_disk):
        """
        Simple FC (First Capable): first server in list that can run the job.
        This roughly mimics the reference FC behaviour.
        """
        for key, s in server_state.items():
            if (
                s["cores"] >= job_cores
                and s["mem"] >= job_mem
                and s["disk"] >= job_disk
            ):
                return key
        return None

    def choose_server_ect(job_submit, job_cores, job_mem, job_disk, est_runtime):
        """
        ECT-style heuristic:
        For each capable server, estimate when the job would complete:
            start_time = max(server.finish_time, job_submit)
            completion_time = start_time + est_runtime
        Choose server with minimum completion_time, with a mild cost tie-breaker.
        """
        best_key = None
        best_score = None

        # Avoid zero runtime (just in case)
        rt = max(est_runtime, 1)

        for key, s in server_state.items():
            if (
                s["cores"] >= job_cores
                and s["mem"] >= job_mem
                and s["disk"] >= job_disk
            ):
                start_time = max(s["finish_time"], job_submit)
                completion_time = start_time + rt

                # Base score: completion time
                score = float(completion_time)

                # Mild cost penalty so cheaper servers are slightly preferred
                score += 0.001 * s["cost"] * rt

                if best_score is None or score < best_score:
                    best_score = score
                    best_key = key

        return best_key

    # ===== Main scheduling loop =====
    while True:
        sock.sendall(b"REDY\n")
        event = recv_line(sock)

        if not event:
            break

        if event == "NONE":
            break

        parts = event.split()

        # Ignore completion and other non-job events
        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
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

            # Select server according to algorithm
            if algo == "fc":
                best_key = choose_server_fc(cores, mem, disk)
            else:
                # Default and "ect": use ECT-style heuristic
                best_key = choose_server_ect(submit_time, cores, mem, disk, est_runtime)

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

            # Estimate when this job will start and finish on the chosen server
            rt = max(est_runtime, 1)
            start_time = max(s_info["finish_time"], submit_time)
            completion_time = start_time + rt

            # Send schedule command
            cmd = f"SCHD {job_id} {s_type} {s_id}\n"
            sock.sendall(cmd.encode())
            _ = recv_line(sock)  # expect "OK"

            # Update our estimated finish time for this server
            s_info["finish_time"] = completion_time

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
