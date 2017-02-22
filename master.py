# class Topic {
#     topic_name
#     num_of_partitions
#     num_of_subscribers
#     subscriptions {
#         {
#             client_name
#             partitions
#         }
#     }
# }

# class Partition {
# 	num_of_messages  # how many msgs in total in this partition
# 	offset
# 	msg_list  # should be stored in file system of servers
# }

# class Client {
#     client_name
#
# }


import sys
import socket
import threading
from queue import Queue
from _thread import *
import json
# import ipdb


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


host = socket.gethostbyname(socket.gethostname())
port = 0
addr = (host,port)
bufsize = 4096

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print("Socket created")
print('Type "server server_name" to start.')
# print('Write a server name')

def get_input(param_count, error_message):
    argument_list = input('$ ').replace('$ ', '').split()

    while len(argument_list) != param_count:
        print(error_message)
        argument_list = input('$ ').replace('$ ', '').split()

    return argument_list

## Read/Write server information file
def save_server():
    server_info = getJson('server_info.json', 'Creating server_info.json')
    name = server_name
    server_info[name] = {
        "ip": host,
        "port": port
    }
    setJson(server_info, 'server_info.json', 'Saving server to server_info.json')


server_input = get_input(2, 'Error: Type "server server_name" to start\n')


server_name = server_input[1]
if server_input[0] == 'server' and server_name:
    try:
        serv.bind(addr)
        host, port = serv.getsockname()
        print('{} at IP address {} and port number: {}'.format(server_name,host,port))
        save_server()
        serv.listen(5)
        print('Waiting for a connection ...')
    except socket.error as e:
        print(str(e))
        sys.exit()


topic_dict = {}

class Topic:
    def init(self, topic_name, num_of_partitions):
        self.topic_name = topic_name
        self.num_of_partitions = num_of_partitions
        self.num_of_subscribers = 0
        self.partition_mapping = {}

        # init partitions
        self.partition_list = []
        for i in range(0, num_of_partitions):
            p = Partition()
            p.index = i
            self.partition_list.append(p)

        print('created a new topic: {}, partitions: {}'.format(topic_name, num_of_partitions))

    def add_subscriber(self, client_name):
        if (self.num_of_subscribers >= self.num_of_partitions):
            print('no partition available, please wait.')
        if self.num_of_subscribers == 0:
            self.partition_mapping[client_name] = self.partition_list

        if client_name in self.partition_mapping:
            print('client already exists')

        last_one = None
        # partition allocations
        for pair in self.partition_mapping:
            if pair.value.length == 1:
                continue
            else:
                last_one = pair.value[-1]
                del pair.value[-1]
        # all of pair.value is a list, which means [last_one] instead of last_one
        self.partition_mapping[client_name] = [last_one]

    def publish(self, client_name, msg, partition_num=None):
        if client_name not in self.partition_mapping:
            print('not a subscriber')

        if partition_num is None:
            for p in self.partition_mapping[client_name]:
                p.add(msg)
        else:
            for p in self.partition_mapping[client_name]:
                if p.index == partition_num:
                    p.add(msg)
                    return

        print('error, not subscribed to this partition')


    def get(self, client_name, partition_num):
        if client_name not in self.partition_mapping:
            print('not a subscriber')

        for p in self.partition_mapping[client_name]:
            if p.index == partition_num:
                p.remove()
                return 'the msg'

        print('error, not subscribed to this partition')


class Partition:
    def __init__(self):
        self.index = -1
        # self.num_of_messages = 0
        # self.offset = -1
        self.msg_list = []

    def reset(self):
        self.__init__()

    def add(self, msg):
        # self.num_of_messages += 1
        self.msg_list.append(msg)

    # better method name
    def remove(self):
        # self.num_of_messages -= 1
        if len(self.msg_list) == 0:
            print('empty')

        first_one = self.msg_list[0]
        del self.msg_list[0]
        return first_one


class Client:
    def __init__(self, client_name):
        self.client_name = client_name


class Master(object):
    def __init__(self):

        def server(conn):
            conn.send(str.encode('Welcome, type your username as "client client_name" to start'))
            username = conn.recv(bufsize).decode()

            ## All the web socket data communication happens below here
            while True:
                raw_data = conn.recv(bufsize)
                data = raw_data.decode()
                split_data = data.split()
                if not data:
                    break
                print('Client {} wrote: {}'.format(username, data))

                if split_data[0] == 'add':
                    servers = ' '.join(split_data[1:])
                    servers = servers.split(') (')
                    print(servers)
                    self.add_servers(servers)
                    self.distribute_server()

                if split_data[0] == '_PASSED_DATA_':
                    passed_data = data.replace('_PASSED_DATA_ ','')
                    print('This is passed right now: '+passed_data)

            conn.close()
            print(username + ' ' + addr[0] + str(addr[1]) + ' closed connection')

        ## Accept connection here and start threading
        while True:
            conn, addr = serv.accept()
            print("Connected with: " + addr[0] + str(addr[1]))

            start_new_thread(server, (conn,))


    def create(self):
        # parser = argparse.ArgumentParser(description='create')
        # parser.add_argument('--topic')
        # parser.add_argument('--partition')
        # args = parser.parse_args(sys.argv[2:])

        t = Topic(t1,3)
        t.init(topic_name='t1', num_of_partitions='4')
        topic_dict[Topic(self)] = t
        print(t)

    def subscribe(self):
        # create a topic
        # t = Topic()
        # t.init('t_1', 2)
        # topic_dict['t_1'] = t


        if args.topic in topic_dict:
            topic_dict[args.topic].add_subsccriber(args.client)
        else:
            print('topic is not exist')

    def add_servers(self, servers):

        ## for easy copy pasting to test:
        ## add (name=S1 ip=192.168.0.107 port=52630) (name=S2 ip=192.168.0.107 port=52633)
        server_info = getJson('server_info.json','Getting server_info.json to add servers')

        for server in servers:
            server = server.replace('(', '').replace(')', '').split()
            name = server[0].replace('name=', '')
            ip = server[1].replace('ip=', '')
            port = server[2].replace('port=', '')
            print(name + ' - ' + ip + ' - ' + port)
            # print(server_info)
            server_info[name] = {
            	"ip": ip,
            	"port": port
            }

        setJson(server_info, 'server_info.json','Adding all servers')

    def get_current_ip(self, server_info):
    	connection_server_name = ''

    	## Find current server name from current ip match
    	for name in server_info:
    		current_ip = socket.gethostbyname(socket.gethostname())

    		if server_info[name]['ip'] == current_ip:
    			connection_server_name = name
    			break
    	return connection_server_name

    def distribute_server(self):
        server_info = getJson('server_info.json','Getting server_info.json to distribute')
        # connection_server_name = self.get_current_ip(server_info)
        servers = list(server_info)

        if server_name in servers:
            servers.remove(server_name)
        # if connection_server_name != server_info[name]:
        print('These are the servers: '+str(servers))
        for server in servers:
            print('This is server: '+str(server))
            host = server_info[server]['ip']
            port = int(server_info[server]['port'])
            addr = (host,port)
            try:
                distribution = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                distribution.bind(addr)
                print('Sending server_info.json to {} at IP address {} and port number: {}'.format(server,host,port))
                passed_data = server_info
                print(passed_data)
                distribution.send(encode('_PASSED_DATA_ '+passed_data))
                distribution.close()
            except socket.error as e:
                print(str(e))



if __name__ == '__main__':
    Master()
