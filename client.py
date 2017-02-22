# Client.py

# publish
# get
# subscribe
# addr_of_master
# create_topic

import json
import socket
# import sys
# from _thread import *

def setJson(dic, fn, msg):
	print(msg)
	with open(fn, 'w') as f:
		json.dump(dic, f)
		f.close()


def getJson(fn, msg):
	try:
		## Reading data back
		with open(fn, 'r') as f:
		     data = json.load(f)
		     f.close()
		     return data
	except:
		setJson({}, fn, msg)
		return {}


def get_current_ip():
	connection_server_name = ''

	## Find current server name from current ip match
	for name in server_info:
		current_ip = socket.gethostbyname(socket.gethostname())

		if server_info[name]['ip'] == current_ip:
			connection_server_name = name
			break
	return connection_server_name


## Get any type of input
def get_input(param_count, error_message):
    argument_list = input('> ').replace('> ', '').split()

    while len(argument_list) != param_count:
        print(error_message)
        argument_list = input('> ').replace('> ', '').split()

    return argument_list

## Get first server message and set username
def start_client():
	info = client.recv(bufsize)
	print(info.decode())

	username_input = get_input(2, 'Error: Type your username as "client client_name" to start.')
	while username_input[0] != 'client':
		print('Error: Type your username as "client client_name" to start.')
		username_input = getInput(2, 'Error: Type your username as "client client_name" to start.')

	username = username_input[1]
	client.send(username.encode())
	return username

## ---------------------------------------------------------------------------------
## MAIN ARGUMENTS
## under here
## ---------------------------------------------------------------------------------

## Read/Write server information file
server_info = getJson('server_info.json', 'writing in server_info.json')
connection_server_name = get_current_ip()

# add (name=itu_server_1 ip=23.253.20.67 port=9998) (name=itu_server_2 ip=10.87.132.1 port=3571)
# port_num = int(sys.argv[1])

host = server_info[connection_server_name]['ip']
port = int(server_info[connection_server_name]['port'])
addr = (host,port)
bufsize = 4096

client = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
client.connect((addr))

host, port = client.getsockname()
print("connected to server {} at {} from port {}".format(connection_server_name,host,port))
print('type "quit" anytime to close connection')

username = start_client()


## Loop while socket connection is open
## Everything that has to happen before the socket connection loop has to happen before this
while True:
    reply = input(username + '> ').replace(username + '> ', '')
    client.send(reply.encode())
    # data = client.recv(bufsize)
    # print(data.decode())
    # if not data:
    #     break
    if reply == 'quit':
        break

client.close()
