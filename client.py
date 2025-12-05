import socket
import sys
from typing import Dict, Any, List, Tuple
from xml.etree import ElementTree

HOST = "localhost"
DEFAULT_PORT = 50000
BUF_SIZE = 8192
DEBUG = False


def log(*args, **kwargs):
    if DEBUG:
        print(*args, **kwargs, file=sys.stderr)


def send_msg(sock: socket.socket, msg: str) -> None:
    if DEBUG:
        print(">>", msg, file=sys.stderr)
    sock.sendall((msg + "\n").encode("utf-8"))


def recv_msg(sock: socket.socket) -> str:
    data = b""
    while True:
        try:
            chunk = sock.recv(BUF_SIZE)
        except (TimeoutError, socket.timeout):
            break
        if not chunk:
            break
        data += chunk
        if len(chunk) < BUF_SIZE:
            break
    text = data.decode().strip()
    if DEBUG:
        print("<<", text, file=sys.stderr)
    return text


def load_system_file(path: str = "ds-system.xml") -> Dict[str, Dict[str, Any]]:
    info: Dict[str, Dict[str, Any]] = {}
    try:
        tree = ElementTree.parse(path)
    except Exception as e:
        log("system file parse error:", e)
        return info

    root = tree.getroot()
    for s in root.iter("server"):
        s_type = s.attrib.get("type")
        if not s_type:
            continue

        def as_int(name: str, default: int = 0) -> int:
            v = s.attrib.get(name)
            if v is None:
                return default
            try:
                return int(v)
            except ValueError:
                return default

        def as_float(name: str, default: float = 0.0) -> float:
            v = s.attrib.get(name)
            if v is None:
                return default
            try:
                return float(v)
            except ValueError:
                return default

        info[s_type] = {
            "limit": as_int("limit", 1),
            "boot": as_int("bootupTime", 0),
            "rate": as_float("hourlyRate", 0.0),
            "cores": as_int("coreCount", as_int("cores", 1)),
            "mem": as_int("memory", 0),
            "disk": as_int("disk", 0),
        }
    return info


def parse_server(line: str) -> Dict[str, Any]:
    p = line.split()
    return {
        "type": p[0],
        "id": int(p[1]),
        "state": p[2],
        "start": int(p[3]),
        "cores": int(p[4]),
        "mem": int(p[5]),
        "disk": int(p[6]),
        "w": int(p[7]),
        "r": int(p[8]),
    }


def get_capable(
    sock: socket.socket, cores: int, mem: int, disk: int
) -> List[Dict[str, Any]]:
    send_msg(sock, f"GETS Capable {cores} {mem} {disk}")
    header = recv_msg(sock)
    if not header.startswith("DATA"):
        log("bad DATA header:", header)
        return []
    _, count_str, _ = header.split()
    count = int(count_str)

    send_msg(sock, "OK")
    data = recv_msg(sock)
    lines = data.splitlines()

    servers: List[Dict[str, Any]] = []
    for line in lines[:count]:
        line = line.strip()
        if line:
            servers.append(parse_server(line))

    send_msg(sock, "OK")
    _ = recv_msg(sock)  # final "."
    return servers


def ejwt(sock: socket.socket, s_type: str, s_id: int) -> int:
    send_msg(sock, f"EJWT {s_type} {s_id}")
    reply = recv_msg(sock)
    try:
        return int(reply.strip())
    except ValueError:
        log("ejwt parse error:", reply)
        return 0


def pick_server(
    sock: socket.socket,
    job: Dict[str, int],
    servers: List[Dict[str, Any]],
    sysinfo: Dict[str, Dict[str, Any]],
) -> Tuple[str, int]:
    need_c = job["cores"]
    need_m = job["mem"]
    need_d = job["disk"]

    immediate: List[Tuple[int, int, str, int]] = []
    delayed: List[Tuple[str, int, Dict[str, Any]]] = []

    for s in servers:
        if (
            s["cores"] >= need_c
            and s["mem"] >= need_m
            and s["disk"] >= need_d
        ):
            si = sysinfo.get(s["type"], {})
            base_cores = si.get("cores", s["cores"])

            if s["state"] in ("idle", "active") and s["w"] == 0:
                leftover = s["cores"] - need_c
                immediate.append((leftover, base_cores, s["type"], s["id"]))
            else:
                delayed.append((s["type"], s["id"], s))

    if immediate:
        immediate.sort(key=lambda x: (x[0], x[1]))
        _, _, t, i = immediate[0]
        return t, i

    best: Tuple[int, int, str, int] | None = None
    for t, i, s in delayed:
        si = sysinfo.get(t, {})
        boot = si.get("boot", 0)
        base_cores = si.get("cores", s["cores"])

        q_wait = ejwt(sock, t, i)

        penalty = 0
        if s["state"] == "inactive":
            penalty = boot
        elif s["state"] == "booting":
            penalty = boot // 2

        score = q_wait + penalty

        if best is None or score < best[0] or (
            score == best[0] and base_cores < best[1]
        ):
            best = (score, base_cores, t, i)

    if best is not None:
        return best[2], best[3]

    first = servers[0]
    return first["type"], first["id"]


def parse_job(line: str) -> Dict[str, int]:
    parts = line.split()
    return {
        "id": int(parts[1]),
        "submit": int(parts[2]),
        "cores": int(parts[3]),
        "mem": int(parts[4]),
        "disk": int(parts[5]),
        "runtime": int(parts[6]),
    }


def get_port_from_args() -> int:
    port = DEFAULT_PORT
    args = sys.argv[1:]
    if not args:
        return port

    if args[0] == "-p" and len(args) >= 2:
        try:
            return int(args[1])
        except ValueError:
            return port

    try:
        return int(args[0])
    except ValueError:
        return port


def main():
    port = get_port_from_args()

    sock = socket.socket()
    sock.settimeout(2)
    sock.connect((HOST, port))

    send_msg(sock, "HELO")
    _ = recv_msg(sock)

    send_msg(sock, "AUTH student")
    _ = recv_msg(sock)

    sysinfo = load_system_file()

    send_msg(sock, "REDY")
    msg = recv_msg(sock)

    while True:
        if not msg:
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            job = parse_job(msg)
            servers = get_capable(sock, job["cores"], job["mem"], job["disk"])
            if not servers:
                send_msg(sock, "REDY")
                msg = recv_msg(sock)
                continue

            s_type, s_id = pick_server(sock, job, servers, sysinfo)
            send_msg(sock, f"SCHD {job['id']} {s_type} {s_id}")
            _ = recv_msg(sock)

        elif msg.startswith("JCPL"):
            pass
        elif msg.startswith("RESF") or msg.startswith("RESR") or msg.startswith("CHKQ"):
            pass
        elif msg.startswith("NONE"):
            send_msg(sock, "QUIT")
            _ = recv_msg(sock)
            break

        send_msg(sock, "REDY")
        msg = recv_msg(sock)

    sock.close()


if __name__ == "__main__":
    main()
