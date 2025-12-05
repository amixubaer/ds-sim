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


def choose_server(servers, need_c, need_m, need_d, est_runtime):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)
    if not eligible:
        return None

    def state_rank(st):
        st = st.lower()
        if st == "active": return 0
        if st == "idle": return 1
        if st == "booting": return 2
        return 3

    est_runtime = max(est_runtime, 1)
    candidates = []

    for s in eligible:
        queue = s["waiting"] + s["running"]
        eff = max(1, s["cores"])
        ect = (queue + 1) * est_runtime / eff
        score = (
            state_rank(s["state"]),
            queue,
            ect,
            -s["cores"],
            -s["memory"],
            s["id"]
        )
        candidates.append((score, s))

    candidates.sort(key=lambda x: x[0])
    return candidates[0][1]



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
