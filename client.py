#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    if not msg.endswith("\n"):
        msg += "\n"
    sock.sendall(msg.encode("ascii"))

def recv_line(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode("ascii", errors="ignore").strip()

def parse_server(line):
    parts = line.split()
    if len(parts) < 8:
        return None
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7])
    }

def choose_server_optimized(servers, need_c, need_m, need_d):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    candidates = []
    for s in eligible:
        core_util = need_c / s["cores"] if s["cores"] > 0 else 0
        queue_impact = s["waiting"] * 100
        state_impact = {"active": 0, "idle": 50, "booting": 150, "inactive": 300}.get(s["state"], 500)
        total_score = (
            (1 - core_util) * 40 +
            queue_impact * 0.4 +
            state_impact * 0.2
        )
        candidates.append((total_score, s))

    candidates.sort(key=lambda s: (
        s[0],
        s[1]["waiting"],
        -s[1]["cores"],
        s[1]["id"],
    ))
    return candidates[0][1]

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=5)
    except Exception:
        return

    send(sock, "HELO")
    if recv_line(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}")
    if recv_line(sock) != "OK":
        sock.close()
        return

    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)
            break

        parts = msg.split()

        if parts[0] in ("JOBN", "JOBP") and len(parts) >= 7:
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")
            header = recv_line(sock)

            if not header.startswith("DATA"):
                continue

            count = int(header.split()[1])
            send(sock, "OK")

            servers = []
            for _ in range(count):
                line = recv_line(sock)
                server = parse_server(line)
                if server:
                    servers.append(server)

            send(sock, "OK")

            while recv_line(sock) != ".":
                pass

            selected = choose_server_optimized(servers, req_cores, req_mem, req_disk)
            if selected is None and servers:
                selected = servers[0]

            if selected:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
                recv_line(sock)

        elif parts[0] == "CHKQ":
            send(sock, "OK")
            recv_line(sock)
            send(sock, "QUIT")
            recv_line(sock)
            break

    sock.close()

if __name__ == "__main__":
    main()
