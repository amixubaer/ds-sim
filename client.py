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

        server_state[(s_type, s_id)] = {
            "cores": cores,
            "mem": mem,
            "disk": disk,
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

        # Handle job completion and other events
        if event.startswith(("JCPL", "RESF", "RESR", "CHKQ")):
            continue

        # New or pre-empted job - THIS IS WHAT YOU RECEIVE FROM SERVER
        if event.startswith("JOBN") or event.startswith("JOBP"):
            if len(parts) < 7:
                continue

            # Parse the job information FROM SERVER
            job_id = parts[2]
            cores = int(parts[3])
            mem = int(parts[4])
            disk = int(parts[5])

            # Find first capable server (simple algorithm)
            best_server = None
            for server_key, server in server_state.items():
                if (server["cores"] >= cores and 
                    server["mem"] >= mem and 
                    server["disk"] >= disk):
                    best_server = server_key
                    break

            if best_server is None:
                # No capable server found
                continue

            server_type, server_id = best_server

            # Send SCHD command TO SERVER (this is what was missing)
            cmd = f"SCHD {job_id} {server_type} {server_id}\n"
            sock.sendall(cmd.encode())
            response = recv_line(sock)  # Expect "OK"

            # Continue the loop
            continue

    # ===== Clean shutdown =====
    sock.sendall(b"QUIT\n")
    try:
        _ = recv_line(sock)
    except Exception:
        pass

    sock.close()


if __name__ == "__main__":
    main()