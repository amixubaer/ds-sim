import socket
import sys
from typing import Dict, Any, List, Tuple
from xml.etree import ElementTree

BUF_SIZE = 8192
DEFAULT_PORT = 50000
HOST = "localhost"
VERBOSE = False  # set True for debugging


def debug(*args, **kwargs):
    if VERBOSE:
        print(*args, **kwargs, file=sys.stderr)


def receive(sock: socket.socket) -> str:
    """Receive a single ds-server message (which may contain multiple lines)."""
    data = b""
    while True:
        try:
            part = sock.recv(BUF_SIZE)
        except (TimeoutError, socket.timeout):
            break
        if not part:
            break
        data += part
        if len(part) < BUF_SIZE:
            break
    message = data.decode().strip()
    if VERBOSE:
        print("Received:", message, file=sys.stderr)
    return message


def send(sock: socket.socket, message: str):
    if VERBOSE:
        print("Sent:", message, file=sys.stderr)
    sock.sendall((message + "\n").encode("utf-8"))


def read_system_info(filename: str = "ds-system.xml") -> Dict[str, Dict[str, Any]]:
    """
    Read static server information produced by ds-server after AUTH.
    We use this to know boot times and core counts for each server type.
    """
    info: Dict[str, Dict[str, Any]] = {}
    try:
        tree = ElementTree.parse(filename)
    except Exception as e:
        debug("Could not read ds-system.xml:", e)
        return info

    root = tree.getroot()
    # ds-system.xml structure: <system><servers><server .../></servers></system>
    for server in root.iter("server"):
        s_type = server.attrib.get("type")
        if not s_type:
            continue

        def _int(attr: str, default: int = 0) -> int:
            val = server.attrib.get(attr)
            try:
                return int(val) if val is not None else default
            except ValueError:
                return default

        def _float(attr: str, default: float = 0.0) -> float:
            val = server.attrib.get(attr)
            try:
                return float(val) if val is not None else default
            except ValueError:
                return default

        info[s_type] = {
            "limit": _int("limit", 1),
            "boot_time": _int("bootupTime", 0),
            "hourly_rate": _float("hourlyRate", 0.0),
            # some versions may use different attribute names
            "cores": _int("coreCount", _int("cores", 1)),
            "memory": _int("memory", 0),
            "disk": _int("disk", 0),
        }
    debug("Loaded system info for types:", list(info.keys()))
    return info


def parse_server_record(line: str) -> Dict[str, Any]:
    parts = line.split()
    record = {
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
    return record


def get_capable_servers(sock: socket.socket, cores: int, mem: int, disk: int) -> List[Dict[str, Any]]:
    """Issue GETS Capable and return a list of server records."""
    send(sock, f"GETS Capable {cores} {mem} {disk}")
    header = receive(sock)
    if not header.startswith("DATA"):
        debug("Unexpected header from GETS:", header)
        return []
    _, n_recs_str, _ = header.split()
    n_recs = int(n_recs_str)

    send(sock, "OK")
    data = receive(sock)
    lines = data.splitlines()
    servers: List[Dict[str, Any]] = []
    for line in lines[:n_recs]:
        if line.strip():
            servers.append(parse_server_record(line))
    send(sock, "OK")
    _ = receive(sock)  # read the terminating '.'
    return servers


def query_ewjt(sock: socket.socket, s_type: str, s_id: int) -> int:
    """Query EJWT for a server (total estimated waiting time of queued jobs)."""
    send(sock, f"EJWT {s_type} {s_id}")
    reply = receive(sock)
    try:
        return int(reply.strip())
    except ValueError:
        debug("Bad EJWT reply:", reply)
        return 0


def select_server_for_job(
    sock: socket.socket,
    job: Dict[str, int],
    servers: List[Dict[str, Any]],
    system_info: Dict[str, Dict[str, Any]],
) -> Tuple[str, int]:
    """
    Scheduling heuristic: EQBF (Earliest-Queue Best-Fit)

    1. Prefer servers that can run the job immediately and have no waiting jobs.
       Among them, choose Best-Fit on available cores.
    2. If none are immediately available, choose the server with the smallest
       (EJWT + boot-penalty). Tie-break on smaller initial core count.
    """
    cores = job["cores"]
    mem = job["memory"]
    disk = job["disk"]

    immediate: List[Tuple[int, int, str, int]] = []  # (leftover_cores, init_cores, type, id)
    deferred: List[Tuple[str, int, Dict[str, Any]]] = []  # (type, id, record)

    for rec in servers:
        s_type = rec["type"]
        s_id = rec["id"]
        state = rec["state"]
        avail_cores = rec["cores"]
        avail_mem = rec["memory"]
        avail_disk = rec["disk"]
        wJobs = rec["wJobs"]

        can_eventually_run = (
            avail_cores >= cores and avail_mem >= mem and avail_disk >= disk
        )

        sys_info = system_info.get(s_type, {})
        init_cores = sys_info.get("cores", avail_cores)

        # Immediate: idle or active, no waiting jobs, and enough free resources
        if state in ("idle", "active") and wJobs == 0 and can_eventually_run:
            leftover = avail_cores - cores
            immediate.append((leftover, init_cores, s_type, s_id))
        else:
            if can_eventually_run:
                deferred.append((s_type, s_id, rec))

    if immediate:
        # Best-fit on leftover cores, then smaller initial cores
        immediate.sort(key=lambda t: (t[0], t[1]))
        _, _, best_type, best_id = immediate[0]
        return best_type, best_id

    # No server can take job immediately: look at queueing times using EJWT
    best_choice = None  # (score, init_cores, type, id)
    for s_type, s_id, rec in deferred:
        sys_info = system_info.get(s_type, {})
        init_cores = sys_info.get("cores", rec["cores"])
        boot_time = sys_info.get("boot_time", 0)

        wait_time = query_ewjt(sock, s_type, s_id)
        penalty = 0
        if rec["state"] == "inactive":
            penalty += boot_time
        elif rec["state"] == "booting":
            # Rough approximation: half boot time remaining
            penalty += boot_time // 2

        score = wait_time + penalty

        if best_choice is None or score < best_choice[0] or (
            score == best_choice[0] and init_cores < best_choice[1]
        ):
            best_choice = (score, init_cores, s_type, s_id)

    if best_choice is not None:
        return best_choice[2], best_choice[3]

    # Fallback: first capable server (should rarely happen)
    first = servers[0]
    return first["type"], first["id"]


def parse_job_message(message: str) -> Dict[str, int]:
    # JOBN jobID submitTime core memory disk estRuntime
    parts = message.split()
    return {
        "id": int(parts[1]),
        "submit": int(parts[2]),
        "cores": int(parts[3]),
        "memory": int(parts[4]),
        "disk": int(parts[5]),
        "est_runtime": int(parts[6]),
    }


def main():
    # Optional: allow port override as first positional arg or -p PORT.
    port = DEFAULT_PORT
    args = sys.argv[1:]
    if args:
        # handle "-p 55555" or just "55555"
        if args[0] == "-p" and len(args) >= 2:
            try:
                port = int(args[1])
            except ValueError:
                pass
        else:
            try:
                port = int(args[0])
            except ValueError:
                pass

    sock = socket.socket()
    sock.settimeout(2)
    sock.connect((HOST, port))

    # Handshake
    send(sock, "HELO")
    _ = receive(sock)
    # Can be replaced with student ID if needed
    send(sock, "AUTH student")
    _ = receive(sock)

    # ds-server now writes ds-system.xml; read static server info
    system_info = read_system_info()

    # Start scheduling loop
    send(sock, "REDY")
    message = receive(sock)

    while True:
        if not message:
            break

        if message.startswith("JOBN") or message.startswith("JOBP"):
            job = parse_job_message(message)
            servers = get_capable_servers(sock, job["cores"], job["memory"], job["disk"])

            if not servers:
                # Fallback: just ask again
                send(sock, "REDY")
                message = receive(sock)
                continue

            s_type, s_id = select_server_for_job(sock, job, servers, system_info)
            send(sock, f"SCHD {job['id']} {s_type} {s_id}")
            _ = receive(sock)  # expect OK

        elif message.startswith("JCPL"):
            # Job completed – we don't reschedule anything here, just continue.
            pass
        elif message.startswith("RESF") or message.startswith("RESR") or message.startswith("CHKQ"):
            # Failure / recovery / queue messages – we ignore for now but keep protocol moving.
            pass
        elif message.startswith("NONE"):
            # No more jobs: terminate
            send(sock, "QUIT")
            _ = receive(sock)  # QUIT back
            break

        # Ask for next event
        send(sock, "REDY")
        message = receive(sock)

    sock.close()


if __name__ == "__main__":
    main()
