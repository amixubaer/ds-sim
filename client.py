import socket

BUF_SIZE = 8192
PORT = 50000 # Change this if using university servers
VERBOSE = True # Controls printing sent and received messages to the terminal, change to False to disable

def receive() -> str:
    data = b''
    while True:
        try:
            part = sock.recv(BUF_SIZE)
        except (TimeoutError, socket.timeout):
            break
        data += part
        if len(part) < BUF_SIZE: # Check if reached end of message
            break
    message = data.decode().strip()
    if VERBOSE:
        print("Received:", message)
    return message
def send(message: str):
    if VERBOSE:
        print("Sent:", message)
    sock.sendall(bytes(f"{message}\n", encoding="utf-8"))
sock = socket.socket()
sock.settimeout(2)
sock.connect(("localhost", PORT))

send("HELO")
receive() # Recieve 'OK' from ds-server
send("AUTH myName") # replace 'myName' with your name
receive() # Recieve 'OK' from ds-server

def get_largest_server_type():
    # Send "GETS" message and pricess the result to
    # identify which server type has the most CPU cores
    # use splitlines() to split up the server lines
    return "super-silk" #temporary hard-coded server type

largest_server_type = get_largest_server_type()
while True:
    send("REDY")
    response = receive()
    if "": # if the response is a JOBN message
        job_details = response.split() # split message into a list of strings
        # Schedule job to the first server of the largest server type
    if "": #if the response is a NONE message
        break

send("QUIT")
receive() 
sock.close()
