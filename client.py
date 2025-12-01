#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"


# -------------------- BASIC SEND/RECV --------------------

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


# -------------------- SERVER RECORD PARSE --------------------

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


# -------------------- GET SERVER LIST --------------------

def get_servers(sock, c, m, d):
    # Try dynamic available servers first
    send(sock, f"GETS Avail {c} {m} {d}")
    header = recv_line(sock)
    count = int(header.split()[1])

    if count > 0:
        send(sock, "OK")
        servers = [parse_server(recv_line(sock)) for _ in range(count)]
        send(sock, "OK")
        recv_line(sock)  # '.'
        return servers

    # Fallback: GETS Capable (static)
    send(sock, f"GETS Capable {c} {m} {d}")
    header = recv_line(sock)
    count = int(header.split()[1])

    send(sock, "OK")
    servers = [parse_server(recv_line(sock)) for _ in range(count)]
    send(sock, "OK")
    recv_line(sock)  # '.'

    return servers


# -------------------- CHOOSE BEST SERVER --------------------

def choose_server(servers, req_cores):
    # Find fastest server type (highest core count)
    max_cores = max(s["cores"] for s in servers)
    fastest = [s for s in servers if s["cores"] == max_cores]

    # Sort fastest type by smallest load and closest core fit
    best = sorted(
        fastest,
        key=lambda s: (
            s["w"] + s["r"],        # lowest load first
            s["cores"] - req_cores, # smallest core waste
            s["id"]                 # deterministic tie-break
        )
    )

    return best[0]


# -------------------- MAIN CLIENT LOOP --------------------

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # Handshake
    send(sock, "HELO")
    recv_line(sock)

    send(sock, f"AUTH {USER}")
    recv_line(sock)

    # Main loop
    while True:
        send(sock, "REDY")
        msg = recv_line(sock)

        if msg == "NONE":
            send(sock, "QUIT")
            recv_line(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            job_id = parts[1]
            req_c = int(parts[3])
            req_m = int(parts[4])
            req_d = int(parts[5])

            # Get servers
            servers = get_servers(sock, req_c, req_m, req_d)

            # Choose best scheduling option
            target = choose_server(servers, req_c)

            # Send SCHD command
            send(sock, f"SCHD {job_id} {target['type']} {target['id']}")
            recv_line(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()


if __name__ == "__main__":
    main()
