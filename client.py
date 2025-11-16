#!/usr/bin/env python3

import socket
import sys

def main():
    sock = None
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect(('localhost', 50000))
        
        # Read initial message from server
        init_msg = sock.recv(1024).decode()
        print(f"Server init: {init_msg.strip()}")
        
        # Send HELO
        sock.send(b"HELO\n")
        response = sock.recv(1024).decode().strip()
        print(f"HELO -> {response}")
        
        # Send AUTH
        sock.send(b"AUTH client\n")
        response = sock.recv(1024).decode().strip()
        print(f"AUTH -> {response}")
        
        # Send REDY
        sock.send(b"REDY\n")
        response = sock.recv(1024).decode().strip()
        print(f"REDY -> {response}")
        
        # Send QUIT
        sock.send(b"QUIT\n")
        response = sock.recv(1024).decode().strip()
        print(f"QUIT -> {response}")
        
    except Exception as e:
        print(f"Error: {e}")
    finally:
        if sock:
            sock.close()

if __name__ == "__main__":
    main()