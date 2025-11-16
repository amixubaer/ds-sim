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
        print(f"HELO raw response: {repr(resp)}")
        if resp != "OK":
            print("HELO failed")
            return

        # 2) AUTH
        username = "48677922"
        cmd = f"AUTH {username}\n"
        sock.sendall(cmd.encode())
        resp = recv_line(sock)
        print(f"AUTH raw response: {repr(resp)}")
        if resp != "OK":
            print("AUTH failed")
            return

        # 3) REDY
        sock.sendall(b"REDY\n")
        resp = recv_line(sock)
        print(f"REDY raw response: {repr(resp)}")

        # 4) QUIT
        sock.sendall(b"QUIT\n")
        resp = recv_line(sock)
        print(f"QUIT raw response: {repr(resp)}")

    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
    finally:
        if sock:
            try:
                sock.close()
            except Exception:
                pass


if __name__ == "__main__":
    main()
