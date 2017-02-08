# Client.py

# publish
# get
# subscribe
# addr_of_master
# create_topic


import socket
import sys
from _thread import *

print("Write the available port number")
port_num = int(input())

host = ''
port = port_num
addr = (host,port)
bufsize = 4096

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect(addr)
host, port = client.getsockname()
print("connected to server at {} from port {}".format(host,port))
print('type "quit" to close connection')

# client.sendall(b'Hello, world ')
# data = client.recv(bufsize)
info = client.recv(bufsize)
print(info.decode())
reply=input('')
client.send(reply.encode())

while True:
    data = client.recv(bufsize)
    if not data:
        break
    if reply == 'quit':
        break
    print(data.decode())
    # conn.sendall(str.encode(reply))
    reply = input('')
    client.send(reply.encode())
    # client.send(str.encode(data))

client.close()