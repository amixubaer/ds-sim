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
        help="Scheduling algorithm name (used in AUTH and by ds_test)",
    )
    parser.add_argument(
        "--port",
        type=int,
        default=50000,
        help="Port ds-server is listening on (default 50000)",
    )
    args = parser.parse_args()

    print(f"Starting client with algo={args.algo}", file=sys.stderr)

    # ===== Connect to server =====
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # ===== Handshake =====
    sock.sendall(b"HELO\n")
    resp1 = recv_line(sock)
    print(f"HELO: {resp1}", file=sys.stderr)

    # ds_test.py expects AUTH <algo>
    sock.sendall(f"AUTH {args.algo}\n".encode())
    resp2 = recv_line(sock)
    print(f"AUTH: {resp2}", file=sys.stderr)

    if resp1 != "OK" or resp2 != "OK":
        print("Handshake FAILED", file=sys.stderr)
        sock.close()
        sys.exit(1)

    print("Handshake SUCCESSFUL", file=sys.stderr)

    # ===== GETS All: get static server list =====
    sock.sendall(b"GETS All\n")
    data_resp = recv_line(sock)
    print(f"GETS All: {data_resp}", file=sys.stderr)

    servers = []
    if data_resp.startswith("DATA"):
        parts = data_resp.split()
        n_recs = int(parts[1])

        # Acknowledge DATA
        sock.sendall(b"OK\n")

        # Receive n_recs server records
        for _ in range(n_recs):
            line = recv_line(sock)
            servers.append(line)
            print(f"Server: {line}", file=sys.stderr)

        # After records, we must send OK, then server replies with '.'
        sock.sendall(b"OK\n")
        dot = recv_line(sock)
        print(f"End of GETS All marker: {dot}", file=sys.stderr)

    print(f"Found {len(servers)} servers", file=sys.stderr)

    # ===== Main scheduling loop (simple FC-style: first capable server) =====
    scheduled_jobs = 0

    while True:
        # Ask for next event
        sock.sendall(b"REDY\n")
        event = recv_line(sock)

        if not event:
            print("Empty event, terminating.", file=sys.stderr)
            break

        if event == "NONE":
            print("No more jobs - simulation complete", file=sys.stderr)
            break

        # Ignore completion and other non-job events
        if (
            event.startswith("JCPL")
            or event.startswith("RESF")
            or event.startswith("RESR")
            or event.startswith("CHKQ")
        ):
            print(f"Ignoring event: {event}", file=sys.stderr)
            continue

        # New or pre-empted job
        if event.startswith("JOBN") or event.startswith("JOBP"):
            parts = event.split()
            # JOBN jobID submitTime core memory disk estRuntime
            if len(parts) >= 7:
                job_id = parts[1]
                submit_time = int(parts[2])   # unused but correct
                cores = int(parts[3])
                memory = int(parts[4])
                disk = int(parts[5])
                est_runtime = int(parts[6])   # unused but parsed

                print(
                    f"Job {job_id} needs: {cores} cores, "
                    f"{memory}MB, {disk}MB",
                    file=sys.stderr,
                )

                # FC-style: choose first capable server from 'servers' list
                scheduled = False
                for server in servers:
                    s_parts = server.split()
                    # Format: type id state curStartTime cores memory disk ...
                    s_type = s_parts[0]
                    s_id = s_parts[1]
                    s_cores = int(s_parts[4])
                    s_memory = int(s_parts[5])
                    s_disk = int(s_parts[6])

                    if (
                        s_cores >= cores
                        and s_memory >= memory
                        and s_disk >= disk
                    ):
                        schedule_cmd = f"SCHD {job_id} {s_type} {s_id}\n"
                        sock.sendall(schedule_cmd.encode())
                        resp = recv_line(sock)
                        print(
                            f"SCHD {job_id} to {s_type} {s_id}: {resp}",
                            file=sys.stderr,
                        )

                        if resp == "OK":
                            scheduled_jobs += 1
                            scheduled = True
                            break

                if not scheduled:
                    print(
                        f"WARNING: Could not find capable server for job {job_id}",
                        file=sys.stderr,
                    )
                    # Fallback – schedule to first server anyway
                    if servers:
                        first = servers[0].split()
                        fallback_cmd = f"SCHD {job_id} {first[0]} {first[1]}\n"
                        sock.sendall(fallback_cmd.encode())
                        resp = recv_line(sock)
                        print(
                            f"Fallback SCHD {job_id} to {first[0]} {first[1]}: {resp}",
                            file=sys.stderr,
                        )

        else:
            print(f"Unknown event: {event}", file=sys.stderr)

    # ===== Clean shutdown =====
    sock.sendall(b"QUIT\n")
    quit_resp = recv_line(sock)
    print(f"QUIT: {quit_resp}", file=sys.stderr)

    sock.close()
    print(f"Successfully scheduled {scheduled_jobs} jobs", file=sys.stderr)


if __name__ == "__main__":
    main()
