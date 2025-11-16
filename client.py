#!/usr/bin/env python3

import socket
import sys

class DSClient:
    def __init__(self, host='localhost', port=50000):
        self.host = host
        self.port = port
        self.socket = None
        
    def connect(self):
        """Establish connection to ds-server"""
        try:
            self.socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.socket.connect((self.host, self.port))
            return True
        except Exception as e:
            print(f"Connection failed: {e}", file=sys.stderr)
            return False
    
    def send_command(self, command):
        """Send command to server and return response"""
        try:
            self.socket.send(f"{command}\n".encode())
            response = self.socket.recv(4096).decode().strip()
            return response
        except Exception as e:
            print(f"Error: {e}", file=sys.stderr)
            return None
    
    def handshake(self):
        """Perform the initial handshake with ds-server"""
        # Step 1: Send HELO
        response = self.send_command("HELO")
        if response != "OK":
            return False
        
        # Step 2: Send AUTH
        response = self.send_command("AUTH client")
        if response != "OK":
            return False
        
        return True

def main():
    client = DSClient()
    
    if not client.connect():
        sys.exit(1)
    
    if client.handshake():
        print("Handshake successful")
        # Send QUIT to end gracefully
        client.send_command("QUIT")
        sys.exit(0)
    else:
        print("Handshake failed")
        sys.exit(1)

if __name__ == "__main__":
    main()