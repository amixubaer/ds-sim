#!/usr/bin/env python3
import socket
import sys

HOST = "127.0.0.1"
PORT = 50000
USER = "ABC"

def send(sock, msg):
    if not msg.endswith('\n'):
        msg += '\n'
    sock.sendall(msg.encode())

def recv_line(sock):
    data = b""
    while not data.endswith(b'\n'):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def parse_server(line):
    parts = line.split()
    try:
        return {
            "type": parts[0],
            "id": int(parts[1]),
            "state": parts[2],
            "cores": int(parts[4]),
            "memory": int(parts[5]),
            "disk": int(parts[6]),
            "waiting": int(parts[7]),
            "running": int(parts[8]),
        }
    except (IndexError, ValueError):
        return None

def choose_server_optimized(servers, need_c, need_m, need_d):
    eligible = []
    for s in servers:
        if s["cores"] >= need_c and s["memory"] >= need_m and s["disk"] >= need_d:
            eligible.append(s)

    if not eligible:
        return None

    state_priority = {"active": 0, "idle": 1, "booting": 2, "inactive": 3}
    
    eligible.sort(key=lambda s: (
        state_priority.get(s["state"], 4),
        s["cores"],
        s["waiting"],
        -s["running"]
    ))
    
    return eligible[0]

def get_capable_servers(sock, cores, memory, disk):
    send(sock, f"GETS Capable {cores} {memory} {disk}\n")
    header = recv_line(sock)
    
    if not header.startswith("DATA"):
        return []
    
    count = int(header.split()[1])
    send(sock, "OK\n")
    
    servers = []
    for i in range(count):
        line = recv_line(sock)
        server = parse_server(line)
        if server:
            servers.append(server)
    
    send(sock, "OK\n")
    
    while recv_line(sock) != ".":
        pass
    
    return servers

def main():
    try:
        sock = socket.create_connection((HOST, PORT), timeout=5)
    except Exception as e:
        print(f"Connection failed: {e}", file=sys.stderr)
        return

    # Handshake
    send(sock, "HELO\n")
    if recv_line(sock) != "OK":
        sock.close()
        return

    send(sock, f"AUTH {USER}\n")
    if recv_line(sock) != "OK":
        sock.close()
        return

    print("Connected to ds-sim", file=sys.stderr)

    # Track current job to avoid loops
    current_job_id = None
    
    # Main event loop
    while True:
        send(sock, "REDY\n")
        msg = recv_line(sock)

        if not msg:
            break

        if msg == "NONE":
            send(sock, "QUIT\n")
            recv_line(sock)
            break

        parts = msg.split()
        
        # Handle NEW job (first submission)
        if parts[0] == "JOBN" and len(parts) >= 7:
            submit_time = parts[1]
            job_id = parts[2]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

            print(f"NEW JOB {job_id}: {req_cores}c {req_mem}m {req_disk}d", file=sys.stderr)
            current_job_id = job_id

            servers = get_capable_servers(sock, req_cores, req_mem, req_disk)
            
            if servers:
                selected = choose_server_optimized(servers, req_cores, req_mem, req_disk)
                if selected:
                    cmd = f"SCHD {job_id} {selected['type']} {selected['id']}\n"
                    print(f"Scheduling: {cmd.strip()}", file=sys.stderr)
                    send(sock, cmd)
                    response = recv_line(sock)
                    print(f"Response: {response}", file=sys.stderr)
                else:
                    selected = servers[0]
                    cmd = f"SCHD {job_id} {selected['type']} {selected['id']}\n"
                    print(f"Fallback: {cmd.strip()}", file=sys.stderr)
                    send(sock, cmd)
                    recv_line(sock)
        
        # Handle RESUBMITTED job (failed/killed job)
        elif parts[0] == "JOBP" and len(parts) >= 7:
            submit_time = parts[1]
            job_id = parts[2]
            req_cores = int(parts[3])
            req_mem = int(parts[4])
            req_disk = int(parts[5])

            print(f"RESUBMITTED JOB {job_id}: {req_cores}c {req_mem}m {req_disk}d", file=sys.stderr)
            
            # For resubmitted jobs, use a simpler first-fit approach
            servers = get_capable_servers(sock, req_cores, req_mem, req_disk)
            
            if servers:
                # Just pick the first capable server for resubmitted jobs
                selected = servers[0]
                cmd = f"SCHD {job_id} {selected['type']} {selected['id']}\n"
                print(f"Rescheduling: {cmd.strip()}", file=sys.stderr)
                send(sock, cmd)
                response = recv_line(sock)
                print(f"Response: {response}", file=sys.stderr)

        # Handle check queue
        elif parts[0] == "CHKQ":
            send(sock, "OK\n")
            recv_line(sock)
            send(sock, "QUIT\n")
            recv_line(sock)
            break

        # Handle other events
        elif parts[0] in ["JCPL", "RESF", "RESR"]:
            print(f"Event: {msg}", file=sys.stderr)

    sock.close()
    print("Simulation complete", file=sys.stderr)

if __name__ == "__main__":
    main()