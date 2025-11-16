#!/usr/bin/env python3

import socket
import sys
import argparse

def recv_line(sock):
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--algo', required=True)
    parser.add_argument('--port', type=int, default=50000)
    args = parser.parse_args()
    
    print(f"Starting client with algo={args.algo}", file=sys.stderr)
    
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(('localhost', args.port))
    
    # Handshake
    sock.sendall(b"HELO\n")
    resp1 = recv_line(sock)
    print(f"HELO: {resp1}", file=sys.stderr)
    
    sock.sendall(f"AUTH {args.algo}\n".encode())
    resp2 = recv_line(sock)
    print(f"AUTH: {resp2}", file=sys.stderr)
    
    if resp1 != "OK" or resp2 != "OK":
        print("Handshake FAILED", file=sys.stderr)
        sock.close()
        sys.exit(1)
    
    print("Handshake SUCCESSFUL", file=sys.stderr)
    
    # Get all servers at start
    sock.sendall(b"GETS All\n")
    data_resp = recv_line(sock)
    print(f"GETS All: {data_resp}", file=sys.stderr)
    
    servers = []
    if data_resp.startswith("DATA"):
        n_recs = int(data_resp.split()[1])
        sock.sendall(b"OK\n")
        
        # Receive all server data
        servers_data = b""
        while True:
            chunk = sock.recv(4096)
            servers_data += chunk
            if b'.\n' in servers_data:
                break
        
        # Parse servers
        for line in servers_data.decode().split('\n'):
            line = line.strip()
            if line and line != '.' and not line.startswith('OK'):
                servers.append(line)
                print(f"Server: {line}", file=sys.stderr)
        
        sock.sendall(b"OK\n")
        recv_line(sock)  # dot
    
    print(f"Found {len(servers)} servers", file=sys.stderr)
    
    # Main scheduling loop
    scheduled_jobs = 0
    while True:
        sock.sendall(b"REDY\n")
        event = recv_line(sock)
        
        if event == "NONE":
            print("No more jobs - simulation complete", file=sys.stderr)
            break
        elif event.startswith("JOBN"):
            parts = event.split()
            if len(parts) >= 7:
                job_id = parts[2]
                cores = int(parts[4])
                memory = int(parts[5])
                disk = int(parts[6])
                
                print(f"Job {job_id} needs: {cores} cores, {memory}MB, {disk}MB", file=sys.stderr)
                
                # Find first capable server
                scheduled = False
                for server in servers:
                    s_parts = server.split()
                    s_type = s_parts[0]
                    s_id = s_parts[1]
                    s_cores = int(s_parts[4])
                    s_memory = int(s_parts[5])
                    s_disk = int(s_parts[6])
                    
                    if s_cores >= cores and s_memory >= memory and s_disk >= disk:
                        schedule_cmd = f"SCHD {job_id} {s_type} {s_id}\n"
                        sock.sendall(schedule_cmd.encode())
                        resp = recv_line(sock)
                        print(f"SCHD {job_id} to {s_type} {s_id}: {resp}", file=sys.stderr)
                        
                        if resp == "OK":
                            scheduled_jobs += 1
                            scheduled = True
                            break
                
                if not scheduled:
                    print(f"WARNING: Could not schedule job {job_id}", file=sys.stderr)
                    # Fallback - try first server anyway
                    if servers:
                        first = servers[0].split()
                        sock.sendall(f"SCHD {job_id} {first[0]} {first[1]}\n".encode())
                        recv_line(sock)
    
    sock.sendall(b"QUIT\n")
    sock.close()
    print(f"Successfully scheduled {scheduled_jobs} jobs", file=sys.stderr)

if __name__ == "__main__":
    main()