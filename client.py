import socket
import sys
from xml.etree import ElementTree
from typing import Dict, Any, List, Tuple

HOST = "localhost"
PORT_DEFAULT = 57922
DEBUG = False


# ------------------------- Utility Layer ------------------------- #

def dbg(*msg):
    if DEBUG:
        print(*msg, file=sys.stderr)


def send(sock: socket.socket, text: str):
    sock.sendall((text + "\n").encode())


def recv(sock: socket.socket) -> str:
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


# ------------------------ System Information ------------------------ #

def system_profile(xml_path="ds-system.xml") -> Dict[str, Dict[str, Any]]:
    profile = {}
    try:
        root = ElementTree.parse(xml_path).getroot()
    except Exception as e:
        dbg("XML error:", e)
        return profile

    for s in root.iter("server"):
        t = s.attrib["type"]

        def as_int(key, default=0):
            try:
                return int(s.attrib.get(key, default))
            except:
                return default

        def as_float(key, default=0.0):
            try:
                return float(s.attrib.get(key, default))
            except:
                return default

        profile[t] = {
            "limit": as_int("limit", 1),
            "boot": as_int("bootupTime", 0),
            "rate": as_float("hourlyRate", 0.0),
            "cores": as_int("coreCount", as_int("cores", 1)),
            "mem": as_int("memory", 0),
            "disk": as_int("disk", 0),
        }

    return profile


# ------------------------ Server Query Logic ------------------------ #

def fetch_capable(sock: socket.socket, job) -> List[Dict[str, Any]]:
    send(sock, f"GETS Capable {job['cores']} {job['memory']} {job['disk']}")
    header = recv(sock)

    if not header.startswith("DATA"):
        return []

    n = int(header.split()[1])
    send(sock, "OK")

    items = []
    for _ in range(n):
        line = recv(sock)
        p = line.split()
        items.append({
            "type": p[0],
            "id": int(p[1]),
            "state": p[2],
            "cores": int(p[4]),
            "memory": int(p[5]),
            "disk": int(p[6]),
            "waitq": int(p[7]),
            "runq": int(p[8]),
        })

    send(sock, "OK")
    _ = recv(sock)
    return items


def query_wait(sock: socket.socket, t: str, sid: int) -> int:
    send(sock, f"EJWT {t} {sid}")
    reply = recv(sock)
    try:
        return int(reply)
    except:
        return 0


# ------------------------ Scheduling Decision ------------------------ #

def pick_server(sock, job, srv_list, sysmeta) -> Tuple[str, int]:

    need_c = job["cores"]
    need_m = job["memory"]
    need_d = job["disk"]

    instant = []
    later = []

    for s in srv_list:
        if s["cores"] < need_c or s["memory"] < need_m or s["disk"] < need_d:
            continue

        meta = sysmeta.get(s["type"], {})
        full_cores = meta.get("cores", s["cores"])
        boot = meta.get("boot", 0)

        if s["state"] in ("idle", "active") and s["waitq"] == 0:
            leftover = s["cores"] - need_c
            instant.append((leftover, full_cores, s["type"], s["id"]))
        else:
            w = query_wait(sock, s["type"], s["id"])
            pen = w
            if s["state"] == "inactive":
                pen += boot
            elif s["state"] == "booting":
                pen += boot // 2
            later.append((pen, full_cores, s["type"], s["id"]))

    if instant:
        instant.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        return instant[0][2], instant[0][3]

    if later:
        later.sort(key=lambda x: (x[0], x[1], x[2], x[3]))
        return later[0][2], later[0][3]

    f = srv_list[0]
    return f["type"], f["id"]


# --------------------------- Job Parsing --------------------------- #

def parse_job(msg: str) -> Dict[str, int]:
    p = msg.split()
    return {
        "id": int(p[1]),
        "submit": int(p[2]),
        "cores": int(p[3]),
        "memory": int(p[4]),
        "disk": int(p[5]),
        "est": int(p[6]),
    }


# ------------------------------ Main Loop ------------------------------ #

def main():
    port = PORT_DEFAULT

    if len(sys.argv) > 1:
        try:
            port = int(sys.argv[1])
        except:
            pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, port))

    send(sock, "HELO")
    recv(sock)

    send(sock, "AUTH Jubaer")
    recv(sock)

    sysmeta = system_profile()

    send(sock, "REDY")
    msg = recv(sock)

    while True:

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            job = parse_job(msg)
            srv = fetch_capable(sock, job)
            if srv:
                t, sid = pick_server(sock, job, srv, sysmeta)
                send(sock, f"SCHD {job['id']} {t} {sid}")
                recv(sock)

        elif msg.startswith("NONE"):
            send(sock, "QUIT")
            recv(sock)
            break

        send(sock, "REDY")
        msg = recv(sock)

    sock.close()


if __name__ == "__main__":
    main()
