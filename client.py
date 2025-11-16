#!/usr/bin/env python3

import socket
import sys

HOST = "localhost"
PORT = 50000

def recv_line(sock):
    """Receive exactly one newline-terminated message"""
    data = b""
    while not data.endswith(b"\n"):
        part = sock.recv(1)
        if not part:
            break
        data += part
    return data.decode().strip()


def main():
    sock = None
    try:
        # Connect to ds-server
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((HOST, PORT))
        print(f"Connected to {HOST}:{PORT}")

        # ---- PROTOCOL HANDSHAKE ----

        # 1. HELO
        sock.sendall(b"HELO\n")
        response = recv_line(sock)
        print(f"HELO -> {response}")
        if response != "OK":
            print("HELO failed")
            return

        # 2. AUTH (use MQ student ID or your actual username)
        username = "48677922"
        sock.sendall(f"AUTH {username}\n".encode())
        response = recv_line(sock)
        print(f"AUTH -> {response}")
        if response != "OK":
            print("AUTH failed")
            return

        # 3. REDY (ask for first scheduling event)
        sock.sendall(b"REDY\n")
        response = recv_line(sock)
        print(f"REDY -> {response}")  # e.g., JOBN … or NONE

        # 4. QUIT (We are stopping here; full solution will loop instead)
        sock.sendall(b"QUIT\n")
        response = recv_line(sock)
        print(f"QUIT -> {response}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)

    finally:
        if sock:
            try:
                sock.close()
            except:
                pass


if __name__ == "__main__":
    main()
