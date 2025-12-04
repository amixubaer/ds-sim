#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    sock.sendall(msg.encode("ascii"))

def receive(sock, timeout=2):
    sock.settimeout(timeout)
    try:
        data = sock.recv(8192)
        if not data:
            return ""
        return data.decode("ascii", errors="ignore").strip()
    except:
        return ""

def parse_server(line):
    parts = line.split()
    s = {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
    }
    return s

def get_all_servers(sock):
    send(sock, "GETS All\n")
    header = receive(sock)
    if not header.startswith("DATA"):
        return []

    count = int(header.split()[1])
    send(sock, "OK\n")

    servers = []
    while len(servers) < count:
        chunk = receive(sock)
        if not chunk:
            break
        for line in chunk.split("\n"):
            line = line.strip()
            if line:
                servers.append(parse_server(line))
                if len(servers) == count:
                    break

    send(sock, "OK\n")
    while True:
        endmsg = receive(sock)
        if "." in endmsg:
            break

    return servers

def find_largest_type(servers):
    if not servers:
        return None, []

    cores_by_type = {}
    for s in servers:
        t = s["type"]
        c = s["cores"]
        if t not in cores_by_type or c > cores_by_type[t]:
            cores_by_type[t] = c

    max_cores = max(cores_by_type.values())
    largest_types = [t for t, c in cores_by_type.items() if c == max_cores]
    largest_types.sort()
    chosen_type = largest_types[0]

    ids = [s["id"] for s in servers if s["type"] == chosen_type]
    ids.sort()
    return chosen_type, ids

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except:
        return

    send(sock, "HELO\n")
    if receive(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    if receive(sock) != "OK":
        sock.close()
        return

    all_servers = get_all_servers(sock)
    largest_type, largest_ids = find_largest_type(all_servers)
    rr_index = 0

    while True:
        send(sock, "REDY\n")
        msg = receive(sock)
        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT\n")
            receive(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            job_id = parts[1]

            if largest_type and largest_ids:
                target_id = largest_ids[rr_index % len(largest_ids)]
                rr_index += 1
                send(sock, f"SCHD {job_id} {largest_type} {target_id}\n")
                receive(sock)
            else:
                # fallback: use GETS Capable for safety if largest_type not available
                req_cores = int(parts[3])
                req_mem = int(parts[4])
                req_disk = int(parts[5])

                send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}\n")
                header = receive(sock)
                if not header.startswith("DATA"):
                    continue

                count = int(header.split()[1])
                send(sock, "OK\n")

                servers = []
                while len(servers) < count:
                    chunk = receive(sock)
                    if not chunk:
                        break
                    for line in chunk.split("\n"):
                        line = line.strip()
                        if line:
                            servers.append(parse_server(line))
                            if len(servers) == count:
                                break

                send(sock, "OK\n")
                while True:
                    endmsg = receive(sock)
                    if "." in endmsg:
                        break

                if servers:
                    s = servers[0]
                    send(sock, f"SCHD {job_id} {s['type']} {s['id']}\n")
                    receive(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            break
        else:
            continue

    sock.close()

if __name__ == "__main__":
    main()
