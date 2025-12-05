import socket
import sys
from typing import Dict, Any, List

HOST = "localhost"
DEFAULT_PORT = 57922
DEBUG = False


def log(*args: Any) -> None:
    if DEBUG:
        print(*args, file=sys.stderr)


def recv_line(sock: socket.socket) -> str:
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def send_line(sock: socket.socket, msg: str) -> None:
    sock.sendall((msg + "\n").encode("utf-8"))


def parse_server(line: str) -> Dict[str, Any]:
    parts = line.split()
    return {
        "type": parts[0],
        "id": int(parts[1]),
        "state": parts[2],
        "curStartTime": int(parts[3]),
        "cores": int(parts[4]),
        "memory": int(parts[5]),
        "disk": int(parts[6]),
        "wJobs": int(parts[7]),
        "rJobs": int(parts[8]),
    }


def get_capable(sock: socket.socket, cores: int, mem: int, disk: int) -> List[Dict[str, Any]]:
    send_line(sock, f"GETS Capable {cores} {mem} {disk}")
    header = recv_line(sock)
    if not header.startswith("DATA"):
        log("unexpected header:", header)
        return []

    _, count_str, _ = header.split()
    count = int(count_str)

    send_line(sock, "OK")

    servers: List[Dict[str, Any]] = []
    for _ in range(count):
        line = recv_line(sock)
        if line:
            servers.append(parse_server(line))

    send_line(sock, "OK")
    _ = recv_line(sock)

    return servers


def parse_job(line: str) -> Dict[str, int]:
    parts = line.split()
    return {
        "id": int(parts[1]),
        "submit": int(parts[2]),
        "cores": int(parts[3]),
        "memory": int(parts[4]),
        "disk": int(parts[5]),
        "est_runtime": int(parts[6]),
    }


def main() -> None:
    port = DEFAULT_PORT
    args = sys.argv[1:]
    if args:
        if args[0] in ("-p", "--port") and len(args) >= 2:
            try:
                port = int(args[1])
            except ValueError:
                pass
        else:
            try:
                port = int(args[0])
            except ValueError:
                pass

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, port))

    send_line(sock, "HELO")
    _ = recv_line(sock)
    send_line(sock, "AUTH Jubaer")
    _ = recv_line(sock)

    send_line(sock, "REDY")
    msg = recv_line(sock)

    while True:
        if not msg:
            break

        if msg.startswith("JOBN") or msg.startswith("JOBP"):
            job = parse_job(msg)
            servers = get_capable(sock, job["cores"], job["memory"], job["disk"])
            if servers:
                s = servers[0]
                send_line(sock, f"SCHD {job['id']} {s['type']} {s['id']}")
                _ = recv_line(sock)
        elif msg.startswith("JCPL"):
            pass
        elif msg.startswith("RESF") or msg.startswith("RESR") or msg.startswith("CHKQ"):
            pass
        elif msg.startswith("NONE"):
            send_line(sock, "QUIT")
            _ = recv_line(sock)
            break

        send_line(sock, "REDY")
        msg = recv_line(sock)

    sock.close()


if __name__ == "__main__":
    main()
