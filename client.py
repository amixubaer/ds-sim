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
            break
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
        "w": int(p[7]) if len(p) > 7 else 0,
        "r": int(p[8]) if len(p) > 8 else 0,
    }


def choose_server(servers, req_cores):
    # Step 1: fastest server type = max cores
    max_cores = max(s["cores"] for s in servers)
    fastest = [s for s in servers if s["cores"] == max_cores]

    # Step 2: sort FASTEST servers only (do not fall back)
    fastest_sorted = sorted(
        fastest,
        key=lambda s: (
            s["w"] + s["r"],            # smallest queue
            s["cores"] - req_cores,     # tightest core fit
            s["id"]                     # stable
        )
    )
    return fastest_sorted[0]


def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    send(sock, "HELO"); recv_line(sock)
    send(sock, f"AUTH {USER}"); recv_line(sock)

    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            p = msg.split()
            job_id = p[1]
            need_c = int(p[3])
            need_m = int(p[4])
            need_d = int(p[5])

            send(sock, f"GETS Capable {need_c} {need_m} {need_d}")
            header = recv_line(sock)
            count = int(header.split()[1])

            send(sock, "OK")
            servers = [parse_server(recv_line(sock)) for _ in range(count)]
            send(sock, "OK")
            recv_line(sock)  # '.'

            chosen = choose_server(servers, need_c)

            send(sock, f"SCHD {job_id} {chosen['type']} {chosen['id']}")
            recv_line(sock)

    sock.close()


if __name__ == "__main__":
    main()
