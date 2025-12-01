#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, txt):
    sock.sendall((txt + "\n").encode())

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
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "memory": int(p[5]),
        "disk": int(p[6]),
        "waiting": int(p[7]) if len(p) > 7 else 0
    }

def choose_server_simple(servers, need_c, need_m, need_d):
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            return s
    return servers[0] if servers else None

def read_data_block(sock):
    header = recv_line(sock)
    if not header.startswith("DATA"):
        return []
    parts = header.split()
    n = int(parts[1])
    send(sock, "OK")
    recs = []
    for _ in range(n):
        line = recv_line(sock)
        recs.append(parse_server(line))
    send(sock, "OK")
    # consume terminating line
    _ = recv_line(sock)
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
            job_id = parts[1]
            need_c = int(parts[3]); need_m = int(parts[4]); need_d = int(parts[5])

            send(s, f"GETS Capable {need_c} {need_m} {need_d}")
            header = recv_line(s)
            if not header.startswith("DATA"):
                continue
            parts = header.split()
            n = int(parts[1])
            send(s, "OK")
            servers = []
            for _ in range(n):
                servers.append(parse_server(recv_line(s)))
            send(s, "OK")
            _ = recv_line(s)  # consume terminating line

            sel = choose_server_simple(servers, need_c, need_m, need_d)
            if sel:
                send(s, f"SCHD {job_id} {sel['type']} {sel['id']}")
                _ = recv_line(s)
        elif msg.startswith("CHKQ"):
            send(s, "OK")
            _ = recv_line(s)
            send(s, "QUIT")
            break

    s.close()

if __name__ == "__main__":
    main()
