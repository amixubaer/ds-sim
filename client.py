#!/usr/bin/env python3

import socket
import sys
import argparse


def recv_line(sock):
    """Receive a single newline-terminated line from the socket."""
    data = b""
    while not data.endswith(b"\n"):
        chunk = sock.recv(1)
        if not chunk:
            break
        data += chunk
    return data.decode().strip()


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "--algo",
        required=True,
        help="Scheduling algorithm name (sent in AUTH).",
    )
    parser.add_argument(
        "-p",
        "--port",
        type=int, 
        default=50000,
        help="Port ds-server is listening on (default 50000).",
    )
    args = parser.parse_args()

    # Connect to server
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect(("localhost", args.port))

    # ===== PROTOCOL STEP 1-2: HELO handshake =====
    sock.sendall(b"HELO\n")
    response = recv_line(sock)
    if response != "OK":
        print(f"HELO failed: {response}", file=sys.stderr)
        sock.close()
        return

    # ===== PROTOCOL STEP 3-4: AUTH handshake =====  
    sock.sendall(f"AUTH {args.algo}\n".encode())
    response = recv_line(sock)
    if response != "OK":
        print(f"AUTH failed: {response}", file=sys.stderr)
        sock.close()
        return

    print("Handshake successful", file=sys.stderr)

    # ===== Get server information =====
    sock.sendall(b"GETS All\n")
    data_line = recv_line(sock)
    
    if not data_line.startswith("DATA"):
        print(f"GETS All failed: {data_line}", file=sys.stderr)
        sock.close()
        return
        
    n_recs = int(data_line.split()[1])
    sock.sendall(b"OK\n")

    # Read server records
    servers = []
    for _ in range(n_recs):
        line = recv_line(sock)
        parts = line.split()
        if len(parts) >= 7:
            servers.append({
                'type': parts[0],
                'id': parts[1], 
                'state': parts[2],
                'curStartTime': parts[3],
                'cores': int(parts[4]),
                'memory': int(parts[5]),
                'disk': int(parts[6])
            })
    
    sock.sendall(b"OK\n")
    dot_line = recv_line(sock)  # Read the final "."

    print(f"Found {len(servers)} servers", file=sys.stderr)

    # ===== PROTOCOL STEP 5: Main event loop =====
    while True:
        # Send REDY to request next event
        sock.sendall(b"REDY\n")
        
        # ===== PROTOCOL STEP 6: Receive event from server =====
        event = recv_line(sock)
        
        if event == "NONE":
            print("No more jobs", file=sys.stderr)
            break
        if not event:
            break

        parts = event.split()
        
        # ===== Handle JOBN event (new job) =====
        if event.startswith("JOBN"):
            if len(parts) < 7:
                print(f"Invalid JOBN: {event}", file=sys.stderr)
                continue
                
            # Parse JOBN format: JOBN submitTime jobID cores memory disk estRunTime
            submit_time = parts[1]
            job_id = parts[2]
            cores = int(parts[3])
            memory = int(parts[4]) 
            disk = int(parts[5])
            est_runtime = parts[6]
            
            print(f"JOBN received: job_id={job_id}, cores={cores}, memory={memory}, disk={disk}", file=sys.stderr)
            
            # Find first capable server
            scheduled = False
            for server in servers:
                if (server['cores'] >= cores and 
                    server['memory'] >= memory and 
                    server['disk'] >= disk):
                    
                    # ===== PROTOCOL STEP 7: Send SCHD command =====
                    cmd = f"SCHD {job_id} {server['type']} {server['id']}\n"
                    sock.sendall(cmd.encode())
                    
                    # ===== PROTOCOL STEP 8: Wait for OK response =====
                    response = recv_line(sock)
                    if response == "OK":
                        print(f"Scheduled job {job_id} to {server['type']} {server['id']}", file=sys.stderr)
                        scheduled = True
                        break
                    else:
                        print(f"SCHD failed: {response}", file=sys.stderr)
            
            if not scheduled:
                print(f"Could not schedule job {job_id}", file=sys.stderr)
                # Send ENQJ to put job in global queue as fallback
                sock.sendall(b"ENQJ GQ\n")
                response = recv_line(sock)
        
        # ===== Handle JOBP event (resubmitted job) =====
        elif event.startswith("JOBP"):
            if len(parts) < 7:
                print(f"Invalid JOBP: {event}", file=sys.stderr)
                continue
                
            # Parse JOBP format: JOBP submitTime jobID cores memory disk estRunTime  
            submit_time = parts[1]
            job_id = parts[2]
            cores = int(parts[3])
            memory = int(parts[4])
            disk = int(parts[5])
            est_runtime = parts[6]
            
            print(f"JOBP received: job_id={job_id}, cores={cores}, memory={memory}, disk={disk}", file=sys.stderr)
            
            # Use first-fit scheduling for resubmitted jobs
            scheduled = False
            for server in servers:
                if (server['cores'] >= cores and 
                    server['memory'] >= memory and 
                    server['disk'] >= disk):
                    
                    cmd = f"SCHD {job_id} {server['type']} {server['id']}\n"
                    sock.sendall(cmd.encode())
                    response = recv_line(sock)
                    if response == "OK":
                        print(f"Rescheduled job {job_id} to {server['type']} {server['id']}", file=sys.stderr)
                        scheduled = True
                        break
            
            if not scheduled:
                print(f"Could not reschedule job {job_id}", file=sys.stderr)
                sock.sendall(b"ENQJ GQ\n")
                response = recv_line(sock)
        
        # ===== Handle other events =====
        elif event.startswith("JCPL"):
            # Job completion - we can ignore for basic scheduling
            print(f"Job completed: {event}", file=sys.stderr)
            
        elif event.startswith("RESF"):
            # Server failure - we can ignore for basic scheduling  
            print(f"Server failed: {event}", file=sys.stderr)
            
        elif event.startswith("RESR"):
            # Server recovery - we can ignore for basic scheduling
            print(f"Server recovered: {event}", file=sys.stderr)
            
        elif event.startswith("CHKQ"):
            # Check queue - no action needed for basic scheduling
            print("Check queue event", file=sys.stderr)
            
        else:
            print(f"Unknown event: {event}", file=sys.stderr)

    # ===== PROTOCOL STEP 12-14: Clean shutdown =====
    print("Sending QUIT", file=sys.stderr)
    sock.sendall(b"QUIT\n")
    response = recv_line(sock)
    sock.close()
    print("Simulation complete", file=sys.stderr)


if __name__ == "__main__":
    main()