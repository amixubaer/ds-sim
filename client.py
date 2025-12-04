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
        "waiting": 0,
        "running": 0,
    }
    if len(parts) >= 9:
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except:
            pass
    return s

def choose_server(servers, need_c, need_m, need_d, est_runtime):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    state_weight = {
        "active": 1.0,
        "idle": 1.2,
        "booting": 2.5,
        "inactive": 50.0,
    }

    candidates = []
    for s in eligible:
        queue = s["waiting"] + s["running"]
        eff_cores = max(1, s["cores"])
        base_ect = (queue + 1) * max(est_runtime, 1) / eff_cores
        penalty = state_weight.get(s["state"].lower(), 3.0)
        ect = base_ect * penalty
        candidates.append((ect, queue, s))

    candidates.sort(
        key=lambda x: (
            x[0],
            x[1],
            -x[2]["cores"],
            -x[2]["memory"],
            x[2]["id"],
        )
    )
    return candidates[0][2]

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
            req_c = int(parts[3])
            req_m = int(parts[4])
            req_d = int(parts[5])
            est_runtime = int(parts[6])

            send(sock, f"GETS Capable {req_c} {req_m} {req_d}\n")
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
                    if line.strip():
                        servers.append(parse_server(line))
                        if len(servers) == count:
                            break

            send(sock, "OK\n")

            while True:
                if "." in receive(sock):
                    break

            selected = choose_server(servers, req_c, req_m, req_d, est_runtime)
            if selected is None:
                selected = servers[0]

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}\n")
            receive(sock)

        elif msg.startswith("CHKQ"):
            send(sock, "OK\n")
            receive(sock)
            send(sock, "QUIT\n")
            break

    sock.close()

if __name__ == "__main__":
    main()
