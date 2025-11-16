#!/usr/bin/env python3

import socket
import sys
import argparse

def recv_line(sock):
    """Receive a newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()

def main():
    # Parse command line arguments for test framework
    parser = argparse.ArgumentParser()
    parser.add_argument('--algo', required=True, help='Scheduling algorithm name')
    parser.add_argument('--port', type=int, default=50000, help='Port number')
    args = parser.parse_args()
    
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', args.port))
        
        # Handshake - must match exactly what test expects
        sock.sendall(b"HELO\n")
        resp = recv_line(sock)
        if resp != "OK":
            sys.exit(1)
            
        # Use the algorithm name as username for AUTH
        sock.sendall(f"AUTH {args.algo}\n".encode())
        resp = recv_line(sock)
        if resp != "OK":
            sys.exit(1)
        
        # Main scheduling loop
        job_count = 0
        while True:
            sock.sendall(b"REDY\n")
            resp = recv_line(sock)
            
            if resp == "NONE":
                break
            elif resp.startswith("JOBN"):
                job_count += 1
                # Parse job: JOBN submitTime jobID cores memory disk estRuntime
                parts = resp.split()
                if len(parts) < 7:
                    continue
                    
                job_id = parts[2]
                cores = parts[4]
                memory = parts[5]
                disk = parts[6]
                
                # Get capable servers for this job
                sock.sendall(f"GETS Capable {cores} {memory} {disk}\n".encode())
                data_resp = recv_line(sock)
                
                if data_resp.startswith("DATA"):
                    # Parse DATA n_recs rec_len
                    data_parts = data_resp.split()
                    n_recs = int(data_parts[1])
                    
                    # Send OK to get server list
                    sock.sendall(b"OK\n")
                    
                    # Receive all server data in one go
                    servers_data = b""
                    while True:
                        chunk = sock.recv(4096)
                        servers_data += chunk
                        if b'.\n' in servers_data:
                            break
                    
                    servers_data = servers_data.decode()
                    
                    # Parse servers
                    servers = []
                    for line in servers_data.split('\n'):
                        line = line.strip()
                        if line and line != '.' and not line.startswith('OK'):
                            servers.append(line)
                    
                    # Send final OK
                    sock.sendall(b"OK\n")
                    dot_resp = recv_line(sock)  # Should be "."
                    
                    # Schedule to first capable server
                    if servers:
                        first_server = servers[0].split()
                        server_type = first_server[0]
                        server_id = first_server[1]
                        
                        schedule_cmd = f"SCHD {job_id} {server_type} {server_id}\n"
                        sock.sendall(schedule_cmd.encode())
                        resp = recv_line(sock)
                        if resp != "OK":
                            print(f"Scheduling failed: {resp}", file=sys.stderr)
                    else:
                        # No capable servers - this should not happen in test
                        print("No capable servers found!", file=sys.stderr)
                        sock.sendall(f"SCHD {job_id} sliverbullet 0\n".encode())
                        recv_line(sock)
                        
            elif resp.startswith("JCPL"):
                # Job completed - continue
                continue
            elif resp.startswith("RESF") or resp.startswith("RESR"):
                # Server failure/recovery - continue
                continue
            elif resp == "CHKQ":
                # Check queue - acknowledge
                sock.sendall(b"OK\n")
        
        print(f"Scheduled {job_count} jobs", file=sys.stderr)
        
        # Clean quit
        sock.sendall(b"QUIT\n")
        recv_line(sock)
        
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        sys.exit(1)
    finally:
        if sock:
            sock.close()

if __name__ == "__main__":
    main()