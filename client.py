import socket
import sys
from xml.etree import ElementTree

HOST = "localhost"
PORT = 57922

def send(sock, msg):
    sock.sendall((msg + "\n").encode())

def recv(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

# metadata is used later for penalty decisions
def load_system(path="ds-system.xml"):
    info = {}
    try:
        root = ElementTree.parse(path).getroot()
    except:
        return info

    for s in root.iter("server"):
        t = s.attrib["type"]
        info[t] = {
            "boot": int(s.attrib.get("bootupTime", 0)),
            "cores": int(s.attrib.get("coreCount", 1)),
        }
    return info

def parse_job(msg):
    p = msg.split()
    return {
        "id": int(p[1]),
        "cores": int(p[3]),
        "memory": int(p[4]),
        "disk": int(p[5]),
    }

def pick_server(sock, capable, job, sysmeta):
    immediate = []
    deferred = []

    for rec in capable:
        p = rec.split()
        stype, sid = p[0], int(p[1])
        state = p[2]

        free_cores = int(p[4])
        if free_cores >= job["cores"]:
            if state in ("idle", "active"):
                immediate.append((free_cores, stype, sid))
            else:
                # not ideal yet; uses wait query only
                send(sock, f"EJWT {stype} {sid}")
                w = int(recv(sock))
                deferred.append((w, stype, sid))

    if immediate:
        immediate.sort(key=lambda x: x[0])
        return immediate[0][1], immediate[0][2]

    if deferred:
        deferred.sort(key=lambda x: x[0])
        return deferred[0][1], deferred[0][2]

    fallback = capable[0].split()
    return fallback[0], int(fallback[1])

def main():
    sock = socket.socket()
    sock.connect((HOST, PORT))

    send(sock, "HELO"); recv(sock)
    send(sock, "AUTH Jubaer"); recv(sock)

    sysinfo = load_system()

    send(sock, "REDY")
    msg = recv(sock)

    while True:
        if msg.startswith("JOBN"):
            job = parse_job(msg)

            send(sock, f"GETS Capable {job['cores']} {job['memory']} {job['disk']}")
            header = recv(sock)

            if header.startswith("DATA"):
                n = int(header.split()[1])
                send(sock, "OK")

                capable = [recv(sock) for _ in range(n)]
                send(sock, "OK")
                recv(sock)

                stype, sid = pick_server(sock, capable, job, sysinfo)
                send(sock, f"SCHD {job['id']} {stype} {sid}")
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
