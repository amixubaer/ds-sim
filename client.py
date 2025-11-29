#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    sock.sendall((msg + '\n').encode())

def recv_line(sock):
    data = b""
    while not data.endswith(b'\n'):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def parse_server(line):
    parts = line.split()
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": int(parts[7]),
    }

def choose_server(servers, need_c, need_m, need_d):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    # Sort by: cores (desc), waiting (asc), memory (desc), id (asc)
    eligible.sort(key=lambda s: (-s["cores"], s["waiting"], -s["memory"], s["id"]))
    return eligible[0]

def main():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))

    # Handshake
    send(sock, "HELO")
    recv_line(sock)  # OK

    send(sock, f"AUTH {USER}")
    recv_line(sock)  # OK

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
            
            # CORRECT: job_id is at index 1, not 2
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

            # Get capable servers
            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")
            header = recv_line(sock)

            if header.startswith("DATA"):
                n_recs = int(header.split()[1])
                send(sock, "OK")

                # Read server records
                servers = []
                for _ in range(n_recs):
                    line = recv_line(sock)
                    servers.append(parse_server(line))

                send(sock, "OK")

                # Read until "."
                while recv_line(sock) != ".":
                    pass

                # Choose server
                selected = choose_server(servers, req_cores, req_mem, req_disk)
                if selected is None:
                    selected = servers[0]

                send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
                recv_line(sock)  # OK

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            recv_line(sock)
            send(sock, "QUIT")
            break

    sock.close()

if __name__ == "__main__":
    main()