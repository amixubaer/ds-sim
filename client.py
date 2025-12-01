#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

class BSock:
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
        line, _, self.buf = self.buf.partition(b"\n")
        return line.decode().strip()

def try_greeting(bs):
    bs.sock.settimeout(0.2)
    try:
        _ = bs.recv_line()
    except Exception:
        pass
    finally:
        bs.sock.settimeout(None)

def parse_srv(line):
    p = line.split()
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "memory": int(p[5]),
        "disk": int(p[6]),
        "waiting": int(p[7]) if len(p) > 7 else 0,
        "running": int(p[8]) if len(p) > 8 else 0
    }

def read_data_block(bs):
    header = bs.recv_line()
    parts = header.split()
    if not parts or parts[0] != "DATA":
        return []
    n = int(parts[1])
    bs.send("OK")
    recs = []
    for _ in range(n):
        line = bs.recv_line()
        if line == "":
            break
        recs.append(parse_srv(line))
    bs.send("OK")
    # consume the terminating line (often ".") or final OK
    _ = bs.recv_line()
    return recs

def get_capable(bs, c, m, d):
    bs.send(f"GETS Capable {c} {m} {d}")
    return read_data_block(bs)

def choose_server(srv_list, need_c, need_m, need_d):
    cand = [s for s in srv_list if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d]
    if not cand:
        cand = srv_list
    def state_pr(s): return {"active":0, "idle":1, "booting":2, "inactive":3}.get(s["state"], 99)
    cand.sort(key=lambda s: (s["cores"] - need_c, state_pr(s), -s["memory"], s["waiting"]))
    return cand[0]

def main():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((HOST, PORT))
    bs = BSock(s)
    try_greeting(bs)
    bs.send("HELO")
    if bs.recv_line() != "OK":
        s.close(); return
    bs.send(f"AUTH {USER}")
    if bs.recv_line() != "OK":
        s.close(); return

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
            sel = choose_server(servers, need_c, need_m, need_d)
            bs.send(f"SCHD {job_id} {sel['type']} {sel['id']}")
            _ = bs.recv_line()
        elif msg.startswith("CHKQ"):
            bs.send("OK")
            _ = bs.recv_line()
            bs.send("QUIT")
            break

    s.close()

if __name__ == "__main__":
    main()
