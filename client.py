#!/usr/bin/env python3
import socket
import sys

HOST = "127.0.0.1"
PORT = 50000
USER = "JUBAER"
BUF = 4096
TIMEOUT = 5.0

def send(sock, msg):
    if not msg.endswith("\n"):
        msg += "\n"
    sock.sendall(msg.encode())

def recv_line(sock):
    data = bytearray()
    sock.settimeout(TIMEOUT)
    try:
        while True:
            part = sock.recv(BUF)
            if not part:
                break
            data.extend(part)
            if b"\n" in part:
                break
    except (socket.timeout, OSError):
        pass
    return data.decode(errors="ignore").strip()

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
        "mem": int(p[5]),
        "disk": int(p[6]),
        "waiting": waiting
    }

def choose_atl(servers, need_c, need_m, need_d):
    state_rank = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}
    best = None
    for s in servers:
        if s["cores"] >= need_c and s["mem"] >= need_m and s["disk"] >= need_d:
            if best is None:
                best = s
                continue
            if s["cores"] > best["cores"]:
                best = s
            elif s["cores"] == best["cores"]:
                if state_rank.get(s["state"], 99) < state_rank.get(best["state"], 99):
                    best = s
                elif state_rank.get(s["state"], 99) == state_rank.get(best["state"], 99):
                    if s["id"] < best["id"]:
                        best = s
    return best

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=TIMEOUT)
    except Exception:
        return

    _ = recv_line(sock)

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
            _ = recv_line(sock)
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

            send(sock, f"GETS Capable {need_c} {need_m} {need_d}")
            header = recv_line(sock)
            if not header or not header.startswith("DATA"):
                continue
            try:
                n = int(header.split()[1])
            except Exception:
                continue

            send(sock, "OK")
            servers = []
            for _ in range(n):
                line = recv_line(sock)
                if not line:
                    break
                rec = parse_server(line)
                if rec:
                    servers.append(rec)

            send(sock, "OK")
            # consume terminating "."
            while True:
                t = recv_line(sock)
                if t == "." or t == "":
                    break

            selected = choose_atl(servers, need_c, need_m, need_d)
            if selected is None and servers:
                selected = servers[0]

            if selected:
                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
                _ = recv_line(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            _ = recv_line(sock)
            send(sock, "QUIT")
            _ = recv_line(sock)
            break

    sock.close()

if __name__ == "__main__":
    main()
