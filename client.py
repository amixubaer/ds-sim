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


def parse_server(line):
    p = line.split()
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "cores": int(p[4]),
        "memory": int(p[5]),
        "disk": int(p[6]),
        "w": int(p[7]),
        "r": int(p[8])
    }


def choose_server(servers, c, m, d):
    capable = [s for s in servers if s["cores"] >= c and s["memory"] >= m and s["disk"] >= d]
    if not capable:
        capable = servers
    max_cores = max(s["cores"] for s in capable)
    best = [s for s in capable if s["cores"] == max_cores]
    best.sort(key=lambda s: (s["w"] + s["r"], -s["memory"], s["id"]))
    return best[0]


def gets(sock, kind, c, m, d):
    send(sock, f"GETS {kind} {c} {m} {d}")
    header = recv_line(sock)
    parts = header.split()
    if parts[0] != "DATA":
        return []
    count = int(parts[1])

    send(sock, "OK")
    servers = []
    for _ in range(count):
        servers.append(parse_server(recv_line(sock)))

    recv_line(sock)  # "."
    send(sock, "OK")
    recv_line(sock)  # "OK"

    return servers


def get_servers(sock, c, m, d):
    s = gets(sock, "Avail", c, m, d)
    if s:
        return s
    return gets(sock, "Capable", c, m, d)


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    send(sock, "HELO")
    recv_line(sock)

    send(sock, f"AUTH {USER}")
    recv_line(sock)

    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)
            break

        if msg.startswith("JCPL") or msg.startswith("RESF") or msg.startswith("RESR"):
            continue

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            p = msg.split()
            job_id = p[1]
            c, m, d = int(p[3]), int(p[4]), int(p[5])

            servers = get_servers(sock, c, m, d)
            if not servers:
                continue

            s = choose_server(servers, c, m, d)

            send(sock, f"SCHD {job_id} {s['type']} {s['id']}")
            recv_line(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
