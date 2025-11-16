#!/usr/bin/env python3

import socket
import sys

HOST = "localhost"
PORT = 50000

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
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")

        # 1) HELO
        sock.sendall(b"HELO\n")
        resp = recv_line(sock)
        print(f"HELO -> {resp}")
        if resp != "OK":
            print("HELO failed")
            return

        # 2) AUTH
        username = "48677922"
        cmd = f"AUTH {username}\n"
        sock.sendall(cmd.encode())
        resp = recv_line(sock)
        print(f"AUTH -> {resp}")
        if resp != "OK":
            print("AUTH failed")
            return

        print("Handshake successful! Starting simulation...")

        # Main simulation loop
        while True:
            # 3) REDY - get next event
            sock.sendall(b"REDY\n")
            resp = recv_line(sock)
            print(f"REDY -> {resp}")

            if resp == "NONE":
                print("No more jobs - simulation complete")
                break
            elif resp.startswith("JOBN"):
                # Parse job: JOBN submitTime jobID estRuntime cores memory disk
                parts = resp.split()
                job_id = parts[1]
                cores = parts[4]
                memory = parts[5]
                disk = parts[6]
                
                print(f"Job {job_id} needs: {cores} cores, {memory}MB memory, {disk}MB disk")
                
                # Get all capable servers
                sock.sendall(f"GETS All\n".encode())
                resp = recv_line(sock)
                print(f"GETS All -> {resp}")
                
                if resp.startswith("DATA"):
                    # Get number of records
                    data_parts = resp.split()
                    n_recs = int(data_parts[1])
                    
                    # Send OK to receive server data
                    sock.sendall(b"OK\n")
                    
                    # Receive all server records
                    servers = []
                    for i in range(n_recs):
                        server_info = recv_line(sock)
                        if server_info and not server_info.startswith('.'):
                            servers.append(server_info)
                            print(f"Server {i}: {server_info}")
                    
                    # Send OK after receiving all servers
                    sock.sendall(b"OK\n")
                    resp = recv_line(sock)  # Should be "."
                    
                    # Simple scheduling: choose first server
                    if servers:
                        first_server = servers[0].split()
                        server_type = first_server[0]
                        server_id = first_server[1]
                        
                        # Schedule job to first server
                        schedule_cmd = f"SCHD {job_id} {server_type} {server_id}\n"
                        sock.sendall(schedule_cmd.encode())
                        resp = recv_line(sock)
                        print(f"SCHD -> {resp}")
                    else:
                        print("No servers available!")
                        break
                        
            elif resp.startswith("JCPL"):
                # Job completed
                print(f"Job completed: {resp}")
            elif resp.startswith("RESF"):
                # Server failed
                print(f"Server failed: {resp}")
            elif resp.startswith("RESR"):
                # Server recovered
                print(f"Server recovered: {resp}")
            elif resp == "CHKQ":
                # Check queue
                print("Check queue requested")
                sock.sendall(b"OK\n")  # Acknowledge

        # 4) QUIT
        sock.sendall(b"QUIT\n")
        resp = recv_line(sock)
        print(f"QUIT -> {resp}")

        print("Simulation finished successfully!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass

if __name__ == "__main__":
    main()