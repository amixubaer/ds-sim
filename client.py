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


def parse_server(line: str):
    """
    Parse a server record line from GETS Capable / GETS All.
    Typical format (brief):
    type id state curStartTime cores mem disk wJobs rJobs [cost ...]
    """
    parts = line.split()
    s_type = parts[0]
    s_id = int(parts[1])
    state = parts[2]
    cores = int(parts[4])
    mem = int(parts[5])
    disk = int(parts[6])

    w_jobs = 0
    r_jobs = 0
    if len(parts) > 7:
        try:
            w_jobs = int(parts[7])
        except ValueError:
            w_jobs = 0
    if len(parts) > 8:
        try:
            r_jobs = int(parts[8])
        except ValueError:
            r_jobs = 0

    return {
        "type": s_type,
        "id": s_id,
        "state": state,
        "cores": cores,
        "memory": mem,
        "disk": disk,
        "wJobs": w_jobs,
        "rJobs": r_jobs,
    }


def choose_server(servers, need_c, need_m, need_d):
    """
    Heuristic:
    - Filter capable servers.
    - Sort by:
        1) fewest (wJobs + rJobs)
        2) smallest (cores - need_c)
        3) largest cores
        4) largest memory
        5) smallest id
    """
    candidates = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            load = s["wJobs"] + s["rJobs"]
            core_gap = s["cores"] - need_c
            # store tuple for sorting plus the record itself
            candidates.append((
                load,                # smaller is better
                core_gap,            # smaller is better
                -s["cores"],         # larger is better
                -s["memory"],        # larger is better
                s["id"],             # smaller is better
                s,
            ))

    if not candidates:
        return None

    candidates.sort(key=lambda t: (t[0], t[1], t[2], t[3], t[4]))
    return candidates[0][5]


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # ===== Handshake =====
    send(sock, "HELO")
    _ = recv_line(sock)  # "OK"

    send(sock, f"AUTH {USER}")
    _ = recv_line(sock)  # "OK"

    # ===== Main loop =====
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
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
           
            # Ask for capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")
            header = recv_line(sock)

            if not header.startswith("DATA"):
                # Something unexpected, just continue to next REDY
                continue

            n_recs = int(header.split()[1])

            # First OK
            send(sock, "OK")

            servers = []
            for _ in range(n_recs):
                line = recv_line(sock)
                servers.append(parse_server(line))

            # Second OK
            send(sock, "OK")

            # Read final "."
            while recv_line(sock) != ".":
                pass

            selected = choose_server(servers, req_cores, req_mem, req_disk)
            if selected is None:
                # Fallback: just use first record if somehow none were "capable"
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
            _ = recv_line(sock)  # "OK"

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            _ = recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
