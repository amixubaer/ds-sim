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

    # Handle initial message if present
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

    print(">>> HANDSHAKE SUCCESSFUL <<<", file=sys.stderr)

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

    # Server state storage
    server_state = {}
    running_jobs = {}  # Track job -> server mapping for completion handling

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()
        if len(rec) < 7:
            continue
            
        s_type = rec[0]
        s_id = rec[1]
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        # Extract cost if available (may be in different positions)
        cost = 1.0
        for i, field in enumerate(rec):
            try:
                cost_val = float(field)
                # Reasonable cost range check
                if 0 <= cost_val <= 1000000:
                    cost = cost_val
                    break
            except ValueError:
                continue

        server_state[(s_type, s_id)] = {
            "cores": cores,
            "mem": mem,
            "disk": disk,
            "cost": cost,
            "current_load": 0.0,  # Current core utilization
            "total_load": 0.0,    # Total core-time accumulated
        }

    # Finish GETS All sequence
    sock.sendall(b"OK\n")
    _ = recv_line(sock)  # final "."

    # ===== Main scheduling loop =====
    while True:
        sock.sendall(b"REDY\n")
        event = recv_line(sock)

        if not event:
            break

        if event == "NONE":
            break

        parts = event.split()

        # Handle job completion
        if event.startswith("JCPL"):
            if len(parts) >= 5:
                job_id = parts[2]
                server_type = parts[3]
                server_id = parts[4]
                
                # Reduce load on the server where job completed
                server_key = (server_type, server_id)
                if server_key in server_state and job_id in running_jobs:
                    job_cores = running_jobs[job_id]["cores"]
                    server_state[server_key]["current_load"] -= job_cores
                    del running_jobs[job_id]
            continue

        # Handle server recovery/failure
        if event.startswith(("RESF", "RESR")):
            # Reset server state on failure/recovery
            if len(parts) >= 4:
                server_type = parts[1]
                server_id = parts[2]
                server_key = (server_type, server_id)
                if server_key in server_state:
                    server_state[server_key]["current_load"] = 0.0
                    server_state[server_key]["total_load"] = 0.0
            continue

        # Handle queue check
        if event.startswith("CHKQ"):
            continue

        # New or pre-empted job
        if event.startswith("JOBN") or event.startswith("JOBP"):
            if len(parts) < 7:
                continue

            # Correct field parsing
            submit_time = int(parts[1])
            job_id = parts[2]  # Fixed: job_id is at index 2
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])
            est_runtime = int(parts[6])

            # Find best server using load-balancing heuristic
            best_server = None
            best_score = float('inf')

            for server_key, server in server_state.items():
                # Check if server can handle the job
                if (server["cores"] >= cores and 
                    server["mem"] >= mem and 
                    server["disk"] >= disk):
                    
                    # Calculate score: current load + cost factor
                    current_load = server["current_load"]
                    cost_factor = server["cost"] * 0.001
                    
                    # Score prioritizes less loaded servers, with cost as tie-breaker
                    score = current_load + cost_factor
                    
                    if score < best_score:
                        best_score = score
                        best_server = server_key

            # Fallback to first capable server if none found
            if best_server is None:
                for server_key, server in server_state.items():
                    if (server["cores"] >= cores and 
                        server["mem"] >= mem and 
                        server["disk"] >= disk):
                        best_server = server_key
                        break

            if best_server is None:
                # No capable server found - this shouldn't happen with GETS All
                continue

            server_type, server_id = best_server

            # Send schedule command
            cmd = f"SCHD {job_id} {server_type} {server_id}\n"
            sock.sendall(cmd.encode())
            response = recv_line(sock)

            if response == "OK":
                # Update server load and track the job
                server_state[best_server]["current_load"] += cores
                server_state[best_server]["total_load"] += cores * est_runtime
                running_jobs[job_id] = {
                    "cores": cores,
                    "server": best_server
                }

    # ===== Clean shutdown =====
    sock.sendall(b"QUIT\n")
    try:
        _ = recv_line(sock)
    except Exception:
        pass

    sock.close()


if __name__ == "__main__":
    main()