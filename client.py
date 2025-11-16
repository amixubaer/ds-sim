#!/usr/bin/env python3

import socket
import sys

def main():
    try:
        # Create socket and connect
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 50000))
        
        # Step 1: Send HELO
        sock.send(b"HELO\n")
        response1 = sock.recv(1024).decode().strip()
        
        # Step 2: Send AUTH
        sock.send(b"AUTH client\n")
        response2 = sock.recv(1024).decode().strip()
        
        # Check if both responses are OK
        if response1 == "OK" and response2 == "OK":
            print("Handshake successful")
            # Send QUIT to end
            sock.send(b"QUIT\n")
            sock.close()
            sys.exit(0)
        else:
            print("Handshake failed")
            sock.close()
            sys.exit(1)
            
    except Exception as e:
        print(f"Error: {e}")
        sys.exit(1)

if __name__ == "__main__":
    main()