import socket
import sys
from xml.etree import ElementTree
from typing import Any, Dict, List, Tuple

HOST = "localhost"
DEFAULT_PORT = 57922


class DSClient:
    def __init__(self, host: str, port: int) -> None:
        self.host = host
        self.port = port
        self.sock: socket.socket | None = None
        self.system_meta: Dict[str, Dict[str, Any]] = {}

    def connect(self) -> None:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.connect((self.host, self.port))
        self.sock = s
        self._handshake()
        self.system_meta = self._read_system()

    def _read_line(self) -> str:
        assert self.sock is not None
        data = b""
        while not data.endswith(b"\n"):
            chunk = self.sock.recv(1)
            if not chunk:
                break
            data += chunk
        return data.decode().strip()

    def _send(self, msg: str) -> None:
        assert self.sock is not None
        self.sock.sendall((msg + "\n").encode("utf-8"))

    def _handshake(self) -> None:
        self._send("HELO")
        _ = self._read_line()
        # personalise with your student ID
        self._send("AUTH 48677922")
        _ = self._read_line()

    def _read_system(self, filename: str = "ds-system.xml") -> Dict[str, Dict[str, Any]]:
        meta: Dict[str, Dict[str, Any]] = {}
        try:
            tree = ElementTree.parse(filename)
        except Exception:
            return meta

        root = tree.getroot()
        for node in root.iter("server"):
            stype = node.attrib.get("type")
            if not stype:
                continue

            def get_int(name: str, default: int = 0) -> int:
                v = node.attrib.get(name)
                try:
                    return int(v) if v is not None else default
                except ValueError:
                    return default

            def get_float(name: str, default: float = 0.0) -> float:
                v = node.attrib.get(name)
                try:
                    return float(v) if v is not None else default
                except ValueError:
                    return default

            meta[stype] = {
                "cores": get_int("coreCount", get_int("cores", 1)),
                "boot": get_int("bootupTime", 0),
                "rate": get_float("hourlyRate", 0.0),
                "limit": get_int("limit", 1),
                "memory": get_int("memory", 0),
                "disk": get_int("disk", 0),
            }
        return meta

    def _get_capable(self, cores: int, mem: int, disk: int) -> List[Dict[str, Any]]:
        self._send(f"GETS Capable {cores} {mem} {disk}")
        header = self._read_line()
        if not header.startswith("DATA"):
            return []

        _, n_str, _ = header.split()
        n = int(n_str)

        self._send("OK")
        records: List[Dict[str, Any]] = []
        for _ in range(n):
            line = self._read_line()
            if not line:
                continue
            records.append(self._parse_server(line))

        self._send("OK")
        _ = self._read_line()  # "."
        return records

    @staticmethod
    def _parse_server(line: str) -> Dict[str, Any]:
        parts = line.split()
        return {
            "type": parts[0],
            "id": int(parts[1]),
            "state": parts[2],
            "start": int(parts[3]),
            "cores": int(parts[4]),
            "memory": int(parts[5]),
            "disk": int(parts[6]),
            "waiting": int(parts[7]),
            "running": int(parts[8]),
        }

    def _query_ewjt(self, stype: str, sid: int) -> int:
        self._send(f"EJWT {stype} {sid}")
        reply = self._read_line()
        try:
            return int(reply.strip())
        except ValueError:
            return 0

    def _pick_target(self, job: Dict[str, int], servers: List[Dict[str, Any]]) -> Tuple[str, int]:
        need_cores = job["cores"]
        need_mem = job["memory"]
        need_disk = job["disk"]

        immediate: List[Tuple[int, int, float, str, int]] = []
        queued: List[Tuple[int, float, int, str, int]] = []

        for s in servers:
            stype = s["type"]
            sid = s["id"]
            free_cores = s["cores"]
            free_mem = s["memory"]
            free_disk = s["disk"]
            state = s["state"]

            if not (free_cores >= need_cores and free_mem >= need_mem and free_disk >= need_disk):
                continue

            meta = self.system_meta.get(stype, {})
            total_cores = meta.get("cores", free_cores)
            boot = meta.get("boot", 0)
            rate = meta.get("rate", 0.0)

            if state in ("idle", "active") and s["waiting"] == 0:
                leftover = free_cores - need_cores
                immediate.append((leftover, total_cores, rate, stype, sid))
            else:
                wait = self._query_ewjt(stype, sid)
                penalty = wait
                if state == "inactive":
                    penalty += boot
                elif state == "booting":
                    penalty += boot // 2
                queued.append((penalty, rate, total_cores, stype, sid))

        if immediate:
            immediate.sort(key=lambda t: (t[0], t[1], t[2], t[3], t[4]))
            _, _, _, stype, sid = immediate[0]
            return stype, sid

        if queued:
            queued.sort(key=lambda t: (t[0], t[1], t[2], t[3], t[4]))
            _, _, _, stype, sid = queued[0]
            return stype, sid

        fallback = servers[0]
        return fallback["type"], fallback["id"]

    @staticmethod
    def _parse_job(line: str) -> Dict[str, int]:
        parts = line.split()
        return {
            "id": int(parts[1]),
            "submit": int(parts[2]),
            "cores": int(parts[3]),
            "memory": int(parts[4]),
            "disk": int(parts[5]),
            "est": int(parts[6]),
        }

    def run(self) -> None:
        if self.sock is None:
            self.connect()

        self._send("REDY")
        msg = self._read_line()

        while msg:
            tag = msg.split()[0]

            if tag in ("JOBN", "JOBP"):
                job = self._parse_job(msg)
                servers = self._get_capable(job["cores"], job["memory"], job["disk"])
                if servers:
                    stype, sid = self._pick_target(job, servers)
                    self._send(f"SCHD {job['id']} {stype} {sid}")
                    _ = self._read_line()

            elif tag == "JCPL":
                pass
            elif tag in ("RESF", "RESR", "CHKQ"):
                pass
            elif tag == "NONE":
                self._send("QUIT")
                _ = self._read_line()
                break

            self._send("REDY")
            msg = self._read_line()

        if self.sock is not None:
            self.sock.close()


def resolve_port() -> int:
    port = DEFAULT_PORT
    if len(sys.argv) > 1:
        args = sys.argv[1:]
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
    return port


def main() -> None:
    port = resolve_port()
    client = DSClient(HOST, port)
    client.run()


if __name__ == "__main__":
    main()
