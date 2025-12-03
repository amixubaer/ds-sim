#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    if not msg.endswith("\n"):
        msg += "\n"
    sock.sendall(msg.encode())

def recv_line(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(4096)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def try_read_initial(sock):
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    finally:
        sock.settimeout(None)

def parse_server(line):
    p = line.split()
    if len(p) < 7:
        return None
    waiting = 0
    if len(p) > 7:
        try:
            waiting = int(p[7])
        except Exception:
            waiting = 0
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "memory": int(p[5]),
        "disk": int(p[6]),
        "waiting": waiting
    }

def choose_server(servers, need_c):
    # Best-fit on cores (minimize cores - need_c >= 0), then fewest waiting,
    # then prefer active/idle, then lower id.
    state_rank = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}
    candidates = []
    for s in servers:
        if s["cores"] >= need_c:
            core_gap = s["cores"] - need_c
            candidates.append((core_gap, s["waiting"], state_rank.get(s["state"], 99), s["id"], s))
    if not candidates:
        return None
    candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
    return candidates[0][4]

def read_data_block(sock):
    header = recv_line(sock)
    if not header.startswith("DATA"):
        return []
    n = int(header.split()[1])
    send(sock, "OK")
    recs = []
    for _ in range(n):
        line = recv_line(sock)
        rec = parse_server(line)
        if rec:
            recs.append(rec)
    send(sock, "OK")
    # consume terminating line ('.')
    while True:
        t = recv_line(sock)
        if t == ".":
            break
    return recs

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    try_read_initial(s)

    send(s, "HELO")
    if recv_line(s) != "OK":
        s.close()
        return

    send(s, f"AUTH {USER}")
    if recv_line(s) != "OK":
        s.close()
        return

    while True:
        send(s, "REDY")
        msg = recv_line(s)
        if not msg:
            break
        if msg == "NONE":
            send(s, "QUIT")
            _ = recv_line(s)
            break
        if msg.startswith(("JCPL", "RESF", "RESR")):
            continue
        if msg.startswith(("JOBN", "JOBP")):
            parts = msg.split()
            if len(parts) < 7:
                continue
            job_id = parts[1]
            need_c = int(parts[3])
            need_m = int(parts[4])
            need_d = int(parts[5])

            send(s, f"GETS Capable {need_c} {need_m} {need_d}")
            header = recv_line(s)
            if not header.startswith("DATA"):
                continue
            n = int(header.split()[1])
            send(s, "OK")
            servers = []
            for _ in range(n):
                line = recv_line(s)
                rec = parse_server(line)
                if rec:
                    servers.append(rec)
            send(s, "OK")
            # consume terminating "."
            while True:
                t = recv_line(s)
                if t == ".":
                    break

            selected = choose_server(servers, need_c)
            if selected is None and servers:
                selected = servers[0]
            if selected:
                send(s, f"SCHD {job_id} {selected['type']} {selected['id']}")
                _ = recv_line(s)
        elif msg.startswith("CHKQ"):
            send(s, "OK")
            _ = recv_line(s)
            send(s, "QUIT")
            _ = recv_line(s)
            break

    s.close()

if __name__ == "__main__":
    main()
