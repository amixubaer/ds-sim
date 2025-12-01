#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

class BufferedSocket:
    def __init__(self, sock):
        self.sock = sock
        self.buf = b""

    def send(self, msg):
        self.sock.sendall((msg + "\n").encode())

    def recv_line(self):
        while b"\n" not in self.buf:
            chunk = self.sock.recv(4096)
            if not chunk:
                if self.buf:
                    line, self.buf = self.buf, b""
                    return line.decode().strip()
                return ""
            self.buf += chunk
        line, sep, rest = self.buf.partition(b"\n")
        self.buf = rest
        return line.decode().strip()

def try_recv_greeting(bs):
    bs.sock.settimeout(0.2)
    try:
        _ = bs.recv_line()
    except Exception:
        pass
    finally:
        bs.sock.settimeout(None)

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

def read_data_block(bs):
    header = bs.recv_line()
    parts = header.split()
    if not parts or parts[0] != "DATA":
        return []
    count = int(parts[1])
    bs.send("OK")
    servers = []
    for _ in range(count):
        line = bs.recv_line()
        if line == "":
            break
        servers.append(parse_server(line))
    # next line expected to be "."
    term = bs.recv_line()
    if term != ".":
        # consume until we see "."
        while term not in ("", "."):
            term = bs.recv_line()
            if term == "":
                break
    bs.send("OK")
    _ = bs.recv_line()  # expect "OK"
    return servers

def get_capable(bs, c, m, d):
    bs.send(f"GETS Capable {c} {m} {d}")
    return read_data_block(bs)

def choose_server(servers, need_c, need_m, need_d):
    cand = [s for s in servers if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d]
    if not cand:
        cand = servers
    cand.sort(key=lambda s: (-s["cores"], s["w"] + s["r"], -s["memory"], s["id"]))
    return cand[0]

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    bs = BufferedSocket(sock)
    try_recv_greeting(bs)

    bs.send("HELO")
    if bs.recv_line() != "OK":
        sock.close()
        return

    bs.send(f"AUTH {USER}")
    if bs.recv_line() != "OK":
        sock.close()
        return

    while True:
        bs.send("REDY")
        msg = bs.recv_line()
        if not msg:
            break
        if msg == "NONE":
            bs.send("QUIT")
            _ = bs.recv_line()
            break
        if msg.startswith(("JCPL", "RESF", "RESR")):
            continue
        if msg.startswith(("JOBN", "JOBP")):
            p = msg.split()
            job_id = p[1]
            need_c = int(p[3]); need_m = int(p[4]); need_d = int(p[5])
            servers = get_capable(bs, need_c, need_m, need_d)
            if not servers:
                continue
            srv = choose_server(servers, need_c, need_m, need_d)
            bs.send(f"SCHD {job_id} {srv['type']} {srv['id']}")
            _ = bs.recv_line()
        elif msg.startswith("CHKQ"):
            bs.send("OK")
            _ = bs.recv_line()
            bs.send("QUIT")
            break

    sock.close()

if __name__ == "__main__":
    main()
