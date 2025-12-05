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


def get_capable_servers(sock, need_c: int, need_m: int, need_d: int):
    send(sock, f"GETS Capable {need_c} {need_m} {need_d}\n")
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

STATE_RANK = {
    "active": 0,
    "booting": 1,
    "idle": 2,
    "inactive": 3,
}

WAIT_WEIGHT = 2.0
RUN_WEIGHT = 0.8

BASE_PENALTY_BOOTING = 0.3
BASE_PENALTY_IDLE = 0.5

THRESH1 = 0.8
THRESH2 = 1.2
THRESH3 = 1.8

def can_run(server, need_c, need_m, need_d):
    return (
        server["cores"] >= need_c
        and server["memory"] >= need_m
        and server["disk"] >= need_d
    )

def server_load(server):
    base = server["waiting"] * WAIT_WEIGHT + server["running"] * RUN_WEIGHT
    return base / server["cores"]

def choose_server(servers, need_c, need_m, need_d, est_runtime):
    candidates = []
    for s in servers:
        if can_run(s, need_c, need_m, need_d):
            candidates.append(s)
    if not candidates:
        return None

    active_loads = [
        server_load(s) for s in candidates if s["state"] == "active"
    ]
    min_active_load = min(active_loads) if active_loads else None

    best = None
    best_key = None

    for s in candidates:
        load = server_load(s)
        state = s["state"]

        if state == "active":
            state_penalty = 0.0
        elif state in ("booting", "idle"):
            if state == "booting":
                base = BASE_PENALTY_BOOTING
            else:
                base = BASE_PENALTY_IDLE

            if min_active_load is None or min_active_load > THRESH3:
                factor = 0.3
            elif min_active_load > THRESH2:
                factor = 0.5
            elif min_active_load > THRESH1:
                factor = 0.8
            else:
                factor = 1.2

            state_penalty = base * factor
        else:
            state_penalty = 2.0

        load_score = load + state_penalty

        key = (
            load_score,
            s["waiting"],
            s["running"],
            STATE_RANK.get(state, 3),
            -s["cores"],
            s["id"],
        )

        if best_key is None or key < best_key:
            best_key = key
            best = s

    return best




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

            servers = get_capable_servers(sock, req_cores, req_mem, req_disk)
            if not servers:
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
