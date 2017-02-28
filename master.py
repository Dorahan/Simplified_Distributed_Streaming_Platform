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


try:
    host = socket.gethostbyname(socket.gethostname())
except:
    host = '23.253.20.67'
# host = socket.gethostbyname(hostname)
print(host)
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

# for topic in topics:
#     t = Topic()
#     t.loadFromJson(topic)
#     topic_dict[t.topic_name] = t

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
            return

        if client_name in self.partition_mapping:
            print('client already exists')
            return

        self.num_of_subscribers += 1

        if self.num_of_subscribers == 1:
            self.partition_mapping[client_name] = self.partition_list
        else:
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

        # Reseted
        for i in range (0, len(self.partition_list)):
            self.partition_list[i] = []

        for i in range(0, self.num_of_partitions):
            self.partition_list[i % self.num_of_subscribers].append(i)


        stream = getJson('data.json','Getting/Setting data.json')

        for topic in stream['Topics']:
            if self.topic_name == topic['topic_name']:
                topic['subscribers'].append(client_name)

        setJson(stream, 'data.json','Adding subscribers to JSON')

    def del_subscriber(self, client_name):
        if client_name not in self.partition_mapping:
            print('Error: Not a subscriber to this topic.')
            return

        if self.num_of_subscribers == 1:
            print('No more subscribers in this topic')
            # Reseted
            for i in range (0, len(self.partition_list)):
                self.partition_list[i] = []
            return




    def publish(self, client_name, msg, partition_num=None):
        if client_name not in self.partition_mapping:
            print('Error: Not a subscriber to this topic.')
            return

        stream = getJson('data.json','Getting/Setting data.json')
        current_topic = None
        for topic in stream['Topics']:
            if self.topic_name == topic['topic_name']:
                current_topic = topic

        if partition_num is None:
            for p in self.partition_mapping[client_name]:
                p.add(msg)
            for i in range(0, self.num_of_partitions):
                current_topic['partitions'][i].append(msg)

        else:
            for p in self.partition_mapping[client_name]:
                if p.index == partition_num:
                    p.add(msg)
            current_topic['partitions'][partition_num].append(msg)

        setJson(stream, 'data.json','Publishing topic to JSON')

        # print('error, not subscribed to this partition')


    def get(self, client_name, partition_num):
        # ipdb.set_trace()
        if client_name not in self.partition_mapping:
            print('Error: Not a subscriber to this topic.')

        for p in self.partition_mapping[client_name]:
            if p.index == partition_num:
                p.remove()
                print(msg)
                return msg

        # print('error, not subscribed to this partition')

    def toJson(self):
        returned_dict = {
            'topic_name': self.topic_name,
            'num_of_partitions': self.num_of_partitions,
            'subscribers': [],
            'partitions': []
        }

        for i in range(0, self.num_of_partitions):
            returned_dict['partitions'].append([])

        return returned_dict


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


class Master(object):
    def __init__(self):
        # self.client_name = 'default_client'

        def server(conn):
            # self.client_name = 'default_client'
            ## Send the first message to client here:
            conn.send(str.encode('Welcome, type your username as "client client_name" to start'))
            # username = conn.recv(bufsize).decode()

            ## ---------------------------------------------------------------------------------
            ## MAIN ARGUMENTS
            ## All the web socket data communication happens below here
            ## ---------------------------------------------------------------------------------

            while True:
                client_name = 'default_client'
                raw_data = conn.recv(bufsize)
                data = raw_data.decode()
                split_data = data.split()
                if not data:
                    break

                if split_data[0] == 'client':
                    self.client_name = split_data[1]
                    conn.send(('').encode())

                if split_data[0] == 'add':
                    servers = ' '.join(split_data[1:])
                    servers = servers.split(') (')
                    print(servers)
                    self.add_servers(servers)
                    self.distribute_server()

                if split_data[0] == '_PASSED_DATA_':
                    str_passed_data = data.replace('_PASSED_DATA_ ','')
                    print(str_passed_data)
                    passed_data = json.loads(str_passed_data)
                    setJson(passed_data, 'server_info.json', 'Saving the above distributed server_info.json')

                if split_data[0] == 'create':
                    print('creating topic')
                    topics = ' '.join(split_data[1:])
                    topics = topics.split(') (')
                    print(topics)
                    self.create(topics)

                if split_data[0] == 'subscribe':
                    topics = ' '.join(split_data[1:])
                    topics = topics.split(') (')
                    self.subscribe(topics)

                if split_data[0] == 'publish':
                    topics = ' '.join(split_data[1:])
                    topics = topics.split(') (')
                    self.publish(topics)

                if split_data[0] == 'get':
                    topics = ' '.join(split_data[1:])
                    topics = topics.split(') (')
                    self.get(topics)

                if split_data[0] == 'unsubscribe':
                    topics = ' '.join(split_data[1:])
                    topics = topics.split(') (')
                    topic_name = self.unsubscribe(topics)
                    feedback = ('unsubscribed {} and partition is moved to others').format(topic_name)
                    # conn.send(feedback.encode())

                print('Client {} wrote: {}'.format(self.client_name, data))

            conn.close()
            print(self.client_name + ' ' + addr[0] + str(addr[1]) + ' closed connection')

        ## Accept connection here and start threading
        while True:
            conn, addr = serv.accept()
            print("Connected with: " + addr[0] + str(addr[1]))

            start_new_thread(server, (conn,))


    def create(self, topics):

        stream = getJson('data.json','Getting/Setting data.json')

        for topic in topics:
            topic = topic.replace('(', '').replace(')', '').split()
            topic_name = topic[0].replace('topic=', '')
            num_of_partitions = int(topic[1].replace('partitions=', ''))
            # print(topic_name + ' - ' + str(num_of_partitions))

            # create a topic
            t = Topic()
            t.init(topic_name, num_of_partitions)
            topic_dict[topic_name] = t

            try:
                stream['Topics'].append(t.toJson())
            except:
                stream['Topics'] = []
                stream['Topics'].append(t.toJson())

        setJson(stream, 'data.json','Saving topics to JSON')


    def subscribe(self, topics):
        for topic in topics:
            topic = topic.replace('(', '').replace(')', '').split()
            topic_name = topic[0].replace('topic=', '')

            if topic_name in topic_dict:
                topic_dict[topic_name].add_subscriber(self.client_name)
            else:
                print('Error: Topic "{}" does not exist').format(topic_name)

    def publish(self, topics):
        for topic in topics:
            topic = topic.replace('(', '').replace(')', '').split()
            topic_name = topic[0].replace('topic=', '')
            partition_num = None
            if len(topic) == 4:
                partition_num = int(topic[1].replace('partition=', ''))
                key = topic[2].replace('key=', '')
                value = int(topic[3].replace('value=', ''))
            else:
                key = topic[1].replace('key=', '')
                value = int(topic[2].replace('value=', ''))

            if topic_name in topic_dict:
                message = {}
                message[key] = value;
                topic_dict[topic_name].publish(self.client_name, message, partition_num)
            else:
                print('Error: Topic "{}" does not exist').format(topic_name)

    def get(self, topics):
        for topic in topics:
            topic = topic.replace('(', '').replace(')', '').split()
            topic_name = topic[0].replace('topic=', '')
            partition_num = topic[1].replace('partition=', '')

            topic_dict[topic_name].get(self.client_name, partition_num)

    def unsubscribe(self, topics):
        for topic in topics:
            topic = topic.replace('(', '').replace(')', '').split()
            topic_name = topic[0].replace('topic=', '')

            if topic_name in topic_dict:
                # topic_dict[topic_name].add_subscriber(self.client_name)
                print('Unsubscribing')
                return topic_name
            else:
                print('Error: Topic "{}" does not exist').format(topic_name)

        return topic_name

    def add_servers(self, servers):

        ## for easy copy pasting to test:
        ## add (name=S1 ip=192.168.0.108 port=50046) (name=S2 ip=192.168.0.107 port=55883)
        ## add (name=S1 ip=23.253.20.67 port=53498) (name=S2 ip=10.210.99.171 port=58013)
        ## add (name=S2 ip=172.20.10.3 port=52185) (name=S1 ip=172.20.10.3 port=51942)
        ## create (topic=extra_t partitions=2) (topic=something partitions=2)
        ## publish (topic=extra_t key=abc value=3)
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

    def distribute_server(self):
        server_info = getJson('server_info.json','Getting server_info.json to distribute')
        # connection_server_name = self.get_current_ip(server_info)
        servers = list(server_info)

        if server_name in servers:
            servers.remove(server_name)
        # if connection_server_name != server_info[name]:
        print('These are the servers: '+str(servers))
        for server in servers:
            # start_new_thread(self.send_servers(servers))
            print('This is server: '+str(server))
            host = server_info[server]['ip']
            port = int(server_info[server]['port'])
            addr = (host,port)
            try:
                print('trying to create socket')
                distributor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                print('trying to connect to addr')
                print(addr)
                distributor.connect(addr)
                print('Sending server_info.json to {} at IP address {} and port number: {}'.format(server,host,port))
                passed_data = server_info
                print(passed_data)
                str_passed_data = json.dumps(passed_data, sort_keys=True)
                print(str_passed_data)
                distributor.send(('_PASSED_DATA_ '+str_passed_data).encode())
                distributor.close()
            except socket.error as e:
                print(str(e))



if __name__ == '__main__':
    Master()
