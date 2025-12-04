#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

BUF_SIZE = 8192

def send(sock, msg: str):
    sock.sendall(msg.encode("ascii"))

def receive(sock, timeout=2) -> str:
    sock.settimeout(timeout)
    try:
        data = sock.recv(BUF_SIZE)
        if not data:
            return ""
        return data.decode("ascii", errors="ignore").strip()
    except:
        return ""

def parse_server(line: str):
    parts = line.split()
    s = {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "waiting": 0,
        "running": 0,
    }
    if len(parts) >= 9:
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except ValueError:
            s["waiting"] = 0
            s["running"] = 0
    return s

def get_servers(sock, qtype: str, need_c: int, need_m: int, need_d: int):
    send(sock, f"GETS {qtype} {need_c} {need_m} {need_d}\n")
    header = receive(sock)
    if not header.startswith("DATA"):
        return []

    tokens = header.split()
    count = int(tokens[1])

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
        if not endmsg:
            break
        if endmsg == "." or endmsg.endswith("."):
            break

    return servers

def choose_server(servers, need_c, need_m, need_d, est_runtime):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)
    if not eligible:
        return None

    short_job = est_runtime <= 500

    if short_job:
        # Prefer low-queue, just-big-enough servers for short jobs
        eligible.sort(
            key=lambda s: (
                s["waiting"] + s["running"],  # smaller queue
                s["cores"],                  # fewer cores (best fit)
                s["memory"],
                s["id"],
            )
        )
    else:
        # Long jobs: favour bigger servers but avoid huge queues
        eligible.sort(
            key=lambda s: (
                s["waiting"] + s["running"],  # still keep queue small
                -s["cores"],                  # more cores
                -s["memory"],
                s["id"],
            )
        )

    return eligible[0]

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
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            # First try servers that can run the job immediately
            servers = get_servers(sock, "Avail", req_cores, req_mem, req_disk)
            if not servers:
                # Fallback to all capable servers
                servers = get_servers(sock, "Capable", req_cores, req_mem, req_disk)

            if not servers:
                # Should not happen, but just in case
                send(sock, "REDY\n")
                continue

            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime)
            if selected is None:
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
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
