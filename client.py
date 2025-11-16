#!/usr/bin/env python3

import socket
import sys

HOST = "localhost"
PORT = 50000

def main():
    sock = None
    try:
        # Connect to ds-server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")

        # ---- EXACT PROTOCOL FROM PAGE 7 ----

        # 1. Client sends HELO
        sock.sendall(b"HELO\n")
        response = sock.recv(1024).decode().strip()
        print(f"1. HELO -> {response}")
        if response != "OK":
            print("HELO failed")
            return

        # 2. Client sends AUTH
        sock.sendall(b"AUTH user\n")
        response = sock.recv(1024).decode().strip()
        print(f"2. AUTH -> {response}")
        if response != "OK":
            print("AUTH failed")
            return

        # 3. Client sends REDY
        sock.sendall(b"REDY\n")
        response = sock.recv(1024).decode().strip()
        print(f"3. REDY -> {response}")

        # Handle the response to REDY (could be JOBN, JCPL, RESF, RESR, CHKQ, NONE)
        if response.startswith("JOBN"):
            # This is a job that needs scheduling
            print("Received a job - would schedule it here")
            # For now, just acknowledge and continue
            pass
        elif response == "NONE":
            print("No more jobs")
        elif response.startswith("JCPL"):
            print("Job completed")
        elif response.startswith("RESF"):
            print("Server failed")
        elif response.startswith("RESR"):
            print("Server recovered")
        elif response == "CHKQ":
            print("Check queue")

        # 4. Client sends QUIT (after handling NONE or when done)
        sock.sendall(b"QUIT\n")
        response = sock.recv(1024).decode().strip()
        print(f"4. QUIT -> {response}")

        print("Protocol completed successfully!")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
    finally:
        if sock:
            sock.close()

if __name__ == "__main__":
    main()