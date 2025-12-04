#!/usr/bin/env python3
import socket

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    if not msg.endswith("\n"):
        msg += "\n"
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
        "type":   parts[0],
        "id":     int(parts[1]),
        "state":  parts[2],
        "cores":  int(parts[4]),
        "memory": int(parts[5]),
        "disk":   int(parts[6]),
        "waiting": 0,
        "running": 0,
    }
    if len(parts) >= 9:
        try:
            s["waiting"] = int(parts[7])
            s["running"] = int(parts[8])
        except ValueError:
            pass
    return s

def choose_server(servers, need_c, need_m, need_d, est_runtime, load_map):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)
    if not eligible:
        return None

    est_runtime = max(1, est_runtime)
    candidates = []

    for s in eligible:
        key = (s["type"], s["id"])
        base_load = load_map.get(key, 0.0)
        eff_cores = max(1, s["cores"])
        queue_jobs = s["waiting"] + s["running"]

        queue_term = queue_jobs * est_runtime / eff_cores
        my_term = est_runtime / eff_cores

        state = s["state"].lower()
        if state == "active":
            weight = 1.0
        elif state == "idle":
            weight = 0.9
        elif state == "booting":
            weight = 1.3
        else:
            weight = 5.0

        score = (base_load + queue_term + my_term) * weight
        fit_penalty = abs(s["cores"] - need_c) * 0.05
        score += fit_penalty

        candidates.append((score, queue_jobs, -s["cores"], -s["memory"], s["id"], s))

    candidates.sort(key=lambda x: (x[0], x[1], x[2], x[3], x[4]))
    return candidates[0][5]

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=1)
    except:
        return

    send(sock, "HELO")
    if receive(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}")
    if receive(sock) != "OK":
        sock.close()
        return

    load_map = {}

    while True:
        send(sock, "REDY")
        msg = receive(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT")
            receive(sock)
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            parts = msg.split()
            job_id = parts[1]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])
            est_runtime = int(parts[6])

            send(sock, f"GETS Capable {req_cores} {req_mem} {req_disk}")
            header = receive(sock)
            if not header.startswith("DATA"):
                continue

            count = int(header.split()[1])
            send(sock, "OK")

            servers = []
            while len(servers) < count:
                chunk = receive(sock)
                if not chunk:
                    break
                for line in chunk.split("\n"):
                    line = line.strip()
                    if not line or line == ".":
                        continue
                    servers.append(parse_server(line))
                    if len(servers) == count:
                        break

            send(sock, "OK")
            while True:
                endmsg = receive(sock)
                if "." in endmsg:
                    break

            selected = choose_server(servers, req_cores, req_mem, req_disk, est_runtime, load_map)
            if selected is None:
                selected = servers[0]

            key = (selected["type"], selected["id"])
            eff_cores = max(1, selected["cores"])
            load_map[key] = load_map.get(key, 0.0) + est_runtime / eff_cores

            send(sock, f"SCHD {job_id} {selected['type']} {selected['id']}")
            receive(sock)

        elif msg.startswith("JCPL"):
            parts = msg.split()
            if len(parts) >= 4:
                stype = parts[-2]
                sid = int(parts[-1])
                key = (stype, sid)
                if key in load_map:
                    load_map[key] = max(0.0, load_map[key] * 0.5)

        elif msg.startswith("CHKQ"):
            send(sock, "OK")
            receive(sock)
            send(sock, "QUIT")
            receive(sock)
            break

        else:
            continue

    sock.close()

if __name__ == "__main__":
    main()
