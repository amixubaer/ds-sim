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

    # Server state: capacity, cost, and heuristic "load"
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
            "load": 0.0,  # heuristic total core-time load
        }
        if cost > max_cost:
            max_cost = cost

    # Finish GETS All sequence
    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    if max_cost <= 0:
        max_cost = 1.0

    # ===== Main scheduling loop: load-balanced heuristic =====
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
            # Format: JOBN submitTime jobID cores memory disk estRuntime
            if len(parts) < 7:
                continue

            submit_time = int(parts[1])  # not used in heuristic, but correct position
            job_id = parts[2]
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # Choose server with minimal heuristic "load" among capable servers.
            best_key = None
            best_score = None

            for key, s in server_state.items():
                if (
                    s["cores"] >= cores
                    and s["mem"] >= mem
                    and s["disk"] >= disk
                ):
                    # Base score = load
                    score = s["load"]
                    # Small cost tie-breaker
                    score += 0.001 * s["cost"] * cores * max(est_runtime, 1)

                    if best_score is None or score < best_score:
                        best_score = score
                        best_key = key

            # Fallback: if no capable server
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

            # Send schedule command
            cmd = f"SCHD {job_id} {s_type} {s_id}\n"
            sock.sendall(cmd.encode())
            _ = recv_line(sock)  # expect "OK"

            # Update heuristic load
            server_state[best_key]["load"] += cores * max(est_runtime, 1)

            continue

        # All other event types ignored

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
