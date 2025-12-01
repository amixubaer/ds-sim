#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    sock.sendall((msg + "\n").encode())

def recv_line(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            return ""
        data += chunk
    return data.decode().strip()

def try_recv_greeting(sock):
    sock.settimeout(0.2)
    try:
        _ = recv_line(sock)
    except Exception:
        pass
    finally:
        sock.settimeout(None)

def parse_server(line):
    p = line.split()
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "memory": int(p[5]),
        "disk": int(p[6]),
        "w": int(p[7]) if len(p) > 7 else 0,
        "r": int(p[8]) if len(p) > 8 else 0
    }

def read_data_block(sock):
    header = recv_line(sock)
    parts = header.split()
    if not parts or parts[0] != "DATA":
        return []
    count = int(parts[1])
    send(sock, "OK")
    servers = []
    for _ in range(count):
        line = recv_line(sock)
        if line == "":
            break
        servers.append(parse_server(line))
    # read terminating "." line
    term = recv_line(sock)
    if term != ".":
        # consume until we see "."
        while term != "." and term != "":
            term = recv_line(sock)
    send(sock, "OK")
    _ = recv_line(sock)  # final OK
    return servers

def choose_server(servers, need_c, need_m, need_d):
    cand = [s for s in servers if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d]
    if not cand:
        cand = servers
    # prefer max cores, then min waiting (w+r), then max memory, then min id
    cand.sort(key=lambda s: (-s["cores"], s["w"] + s["r"], -s["memory"], s["id"]))
    return cand[0]

def get_capable(sock, c, m, d):
    send(sock, f"GETS Capable {c} {m} {d}")
    return read_data_block(sock)

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    try_recv_greeting(s)

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
            recv_line(s)
            break
        if msg.startswith(("JCPL", "RESF", "RESR")):
            continue
        if msg.startswith(("JOBN", "JOBP")):
            p = msg.split()
            job_id = p[1]
            need_c = int(p[3]); need_m = int(p[4]); need_d = int(p[5])
            servers = get_capable(s, need_c, need_m, need_d)
            if not servers:
                continue
            srv = choose_server(servers, need_c, need_m, need_d)
            send(s, f"SCHD {job_id} {srv['type']} {srv['id']}")
            recv_line(s)
        elif msg.startswith("CHKQ"):
            send(s, "OK")
            recv_line(s)
            send(s, "QUIT")
            break

    s.close()

if __name__ == "__main__":
    main()
