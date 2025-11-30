#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"


def send(sock, msg: str) -> None:
    """Send a single line (with newline) to the server."""
    sock.sendall((msg + "\n").encode())


def recv_line(sock) -> str:
    """Receive a single newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # ===== Handshake =====
    send(sock, "HELO")
    _ = recv_line(sock)  # expect "OK"

    send(sock, f"AUTH {USER}")
    _ = recv_line(sock)  # expect "OK"

    # ===== Get static server info (GETS All) =====
    send(sock, "GETS All")
    header = recv_line(sock)

    parts = header.split()
    if not header.startswith("DATA") or len(parts) < 3:
        # Clean quit if something weird happens
        send(sock, "QUIT")
        recv_line(sock)
        sock.close()
        return

    n_recs = int(parts[1])

    # First OK
    send(sock, "OK")

    # Parse all servers: record cores, mem, disk
    # And initialise a "ready time" estimate for each server (for ECT)
    servers = []  # list of (type, id, cores, mem, disk)
    ready_time = {}  # (type, id) -> time when server becomes free in our model

    for _ in range(n_recs):
        line = recv_line(sock)
        rec = line.split()
        s_type = rec[0]
        s_id = int(rec[1])
        cores = int(rec[4])
        mem = int(rec[5])
        disk = int(rec[6])

        servers.append((s_type, s_id, cores, mem, disk))
        ready_time[(s_type, s_id)] = 0  # initially all free at time 0

    # Second OK
    send(sock, "OK")
    # Read final "."
    while recv_line(sock) != ".":
        pass

    # ===== Main scheduling loop (ECT-style) =====
    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            
            submit_time = int(parts[1])
            job_id = parts[1]          
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            # Choose server by Earliest Completion Time (ECT)
            best_server = None
            best_finish_time = None

            for (s_type, s_id, cores, mem, disk) in servers:
                # Must be capable
                if cores < req_cores or mem < req_mem or disk < req_disk:
                    continue

                key = (s_type, s_id)
                # When can this server actually start the new job?
                start_time = max(ready_time[key], submit_time)
                finish_time = start_time + est_runtime

                if best_finish_time is None or finish_time < best_finish_time:
                    best_finish_time = finish_time
                    best_server = (s_type, s_id)
                # Tie-break: if same finish time, prefer more cores, then more mem, then smaller id
                elif finish_time == best_finish_time:
                    # Need cores/mem for tie-break; look them up
                    bt, bi = best_server
                    # find their cores/mem from servers list
                    # (small list, so a simple scan is fine)
                    best_cores = best_mem = 0
                    for (t2, i2, c2, m2, d2) in servers:
                        if t2 == bt and i2 == bi:
                            best_cores, best_mem = c2, m2
                            break

                    if cores > best_cores or (cores == best_cores and mem > best_mem) or (
                        cores == best_cores and mem == best_mem and s_id < bi
                    ):
                        best_server = (s_type, s_id)

            # Fallback: if somehow no capable server found (shouldn't happen), pick the first
            if best_server is None:
                s_type, s_id, _, _, _ = servers[0]
            else:
                s_type, s_id = best_server

            # Update our ready_time model for that server
            key = (s_type, s_id)
            start_time = max(ready_time[key], submit_time)
            ready_time[key] = start_time + est_runtime

            # Send schedule command
            send(sock, f"SCHD {job_id} {s_type} {s_id}")
            _ = recv_line(sock)  # expect "OK"

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            _ = recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
