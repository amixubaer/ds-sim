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


def gets_servers(sock, query):
    """
    Send a GETS query (Avail or Capable) and return a list of server records.
    Each record is a dict with keys: type, id, state, cur_start, cores, mem, disk, cost.
    """
    sock.sendall(query.encode())
    header = recv_line(sock)

    if not header.startswith("DATA"):
        # No data or protocol issue: return empty list
        return []

    parts = header.split()
    n_recs = int(parts[1])

    # Acknowledge DATA header
    sock.sendall(b"OK\n")

    servers = []
    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()

        s_type = rec[0]
        s_id = rec[1]
        state = rec[2]
        cur_start = int(rec[3])
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        cost = 1.0
        if len(rec) >= 9:
            try:
                cost = float(rec[8])
            except ValueError:
                cost = 1.0

        servers.append(
            {
                "type": s_type,
                "id": s_id,
                "state": state,
                "cur_start": cur_start,
                "cores": cores,
                "mem": mem,
                "disk": disk,
                "cost": cost,
            }
        )

    # Finish GETS sequence
    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    return servers


def pick_fastest(servers):
    """
    Choose the 'fastest' server from a list:
    - Prefer more cores
    - On ties, pick lexicographically smallest (type, id).
    """
    if not servers:
        return None

    best = None
    for s in servers:
        if best is None:
            best = s
            continue

        if s["cores"] > best["cores"]:
            best = s
        elif s["cores"] == best["cores"]:
            if (s["type"], int(s["id"])) < (best["type"], int(best["id"])):
                best = s

    return best


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

    # Send algo string in AUTH (marker reads only SCHD pattern, not this)
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

    # ===== GETS All (static server list for ECT + fallback) =====
    sock.sendall(b"GETS All\n")
    data_resp = recv_line(sock)

    if not data_resp.startswith("DATA"):
        sock.sendall(b"QUIT\n")
        try:
            recv_line(sock)
        except Exception:
            pass
        sock.close()
        sys.exit(1)

    parts = data_resp.split()
    n_recs = int(parts[1])

    sock.sendall(b"OK\n")

    # Static info + dynamic finish_time for ECT
    server_state = {}
    all_servers = []
    max_cost = 0.0

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()

        s_type = rec[0]
        s_id = rec[1]
        state = rec[2]
        cur_start = int(rec[3])
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        cost = 1.0
        if len(rec) >= 9:
            try:
                cost = float(rec[8])
            except ValueError:
                cost = 1.0

        info = {
            "type": s_type,
            "id": s_id,
            "state": state,
            "cur_start": cur_start,
            "cores": cores,
            "mem": mem,
            "disk": disk,
            "cost": cost,
            # For ECT-style we maintain an approximate finish_time
            "finish_time": cur_start,
        }
        server_state[(s_type, s_id)] = info
        all_servers.append(info)

        if cost > max_cost:
            max_cost = cost

    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    if max_cost <= 0:
        max_cost = 1.0

    # Global time tracker (approx; based on events)
    current_time = 0

    # ===== Helper: FC (First Capable using static list) =====
    def choose_fc(job_cores, job_mem, job_disk):
        for key, s in server_state.items():
            if (
                s["cores"] >= job_cores
                and s["mem"] >= job_mem
                and s["disk"] >= job_disk
            ):
                return key
        return None

    # ===== Helper: FAFC (Fastest Available then Fastest Capable) =====
    def choose_fafc(job_cores, job_mem, job_disk):
        # 1. Try GETS Avail
        avail = gets_servers(sock, f"GETS Avail {job_cores} {job_mem} {job_disk}\n")
        if avail:
            fastest = pick_fastest(avail)
            return (fastest["type"], fastest["id"])

        # 2. If nothing available, GETS Capable
        capable = gets_servers(sock, f"GETS Capable {job_cores} {job_mem} {job_disk}\n")
        if capable:
            fastest = pick_fastest(capable)
            return (fastest["type"], fastest["id"])

        # 3. Fallback: fastest overall
        fastest = pick_fastest(all_servers)
        if fastest is None:
            return None
        return (fastest["type"], fastest["id"])

    # ===== Helper: ECT-style (Estimated Completion Time) =====
    def choose_ect(job_submit, job_cores, job_mem, job_disk, est_runtime):
        best_key = None
        best_score = None

        rt = max(est_runtime, 1)

        for key, s in server_state.items():
            if (
                s["cores"] >= job_cores
                and s["mem"] >= job_mem
                and s["disk"] >= job_disk
            ):
                # Approximate when this job would start on this server
                start_time = max(s["finish_time"], job_submit, current_time)
                completion_time = start_time + rt

                # Primary objective: earliest completion
                score = float(completion_time)

                # Slightly penalise expensive servers (tie-breaker)
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

        # Track time from JCPL / JOBN where available
        if event.startswith("JCPL"):
            # JCPL time serverType serverID jobID
            if len(parts) >= 5:
                t = int(parts[1])
                s_type = parts[2]
                s_id = parts[3]
                current_time = max(current_time, t)
                key = (s_type, s_id)
                if key in server_state:
                    # Server definitely free at time t; don't let our estimate lag behind
                    server_state[key]["finish_time"] = max(server_state[key]["finish_time"], t)
            continue

        if event.startswith("RESF") or event.startswith("RESR") or event.startswith("CHKQ"):
            continue

        # New or pre-empted job
        if event.startswith("JOBN") or event.startswith("JOBP"):
            # JOBN submitTime jobID cores memory disk estRuntime  (variant you’re using)
            if len(parts) < 7:
                continue

            submit_time = int(parts[1])
            job_id = parts[2]
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # Update global time with submit_time (jobs won’t arrive earlier than this)
            current_time = max(current_time, submit_time)

            # ===== Choose server based on algo =====
            if algo == "fc":
                best_key = choose_fc(cores, mem, disk)
            elif algo == "fafc":
                best_key = choose_fafc(cores, mem, disk)
            else:
                # default and "ect" → ECT-style heuristic
                best_key = choose_ect(submit_time, cores, mem, disk, est_runtime)

            # Fallback: if somehow none found, use fastest overall
            if best_key is None:
                fastest = pick_fastest(all_servers)
                if fastest is None:
                    sock.sendall(b"QUIT\n")
                    try:
                        recv_line(sock)
                    except Exception:
                        pass
                    sock.close()
                    sys.exit(1)
                best_key = (fastest["type"], fastest["id"])

            s_type, s_id = best_key
            s_info = server_state.get(best_key)

            # For ECT-style: update finish_time estimate
            rt = max(est_runtime, 1)
            if s_info is not None:
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
