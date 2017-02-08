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
import csv

host = ''
port = 0
addr = (host,port)
bufsize = 4096

serv = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
print("Socket created")
print("This is a streaming platform")

print('Write a server name')
server_name=input('$ ').replace('$ ','')
print('Use "server {}" command to start'.format(server_name))
if input('$ ') == 'server {}'.format(server_name):
    try:
        serv.bind(addr)
        host, port = serv.getsockname()
        print('{} at IP address {} and port number: {}'.format(server_name,host,port))
        serv.listen(5)
        print('Waiting for a connection ...')
    except socket.error as e:
        print(str(e))
        sys.exit()
else:
    print('Unknown command.\nUse "server server_name" command to start\n')

topic_dict = {}
subcmd = ('create', 'publish', 'add', 'get')
secondcmd = ('topic', 't1')

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
            conn.send(str.encode("Welcome, type your username\n"))
            username_info = conn.recv(bufsize)
            username = username_info.decode("utf-8")
            Master.user_file(username)
            chev = "> "
            conn.send(str.encode(username + chev))

            while True:
                data = conn.recv(bufsize)
                if not data:
                    break
                # if data.decode() == 'create':
                #     print('created test')
                #     Master.create(self)
                #     conn.send(str.encode('created test worked'))
                if data.decode() in subcmd:
                    print('valid subcommand')
                if data.decode() == subcmd + secondcmd:
                    print('yes')
                print('{} wrote: {}'.format(username, data))
                conn.send(str.encode(username + chev))

            conn.close()
            print(username + ' ' + addr[0] + str(addr[1]) + ' closed connection')

        while True:
            conn, addr = serv.accept()
            print("Connected with: " + addr[0] + str(addr[1]))

            start_new_thread(server, (conn,))

    # Check if username exists
    def user_file(username):
        ''' Check if a file exists and is accessible. '''

        try:
            with open('info_user.csv') as f:
                print('Running user_file.csv now ...')
                pass
        except IOError as e:
            with open('info_user.csv','w') as f:
                print('creating file')
                print('Running user_file.csv now ...')
                f.close()

        if True:
            with open('info_user.csv', 'r') as f:
                reader = csv.reader(f, delimiter=',')
                for name in reader:
                    if name == username:
                        print(username, 'named user exists')
                        f.close()
                        return True
                    else:
                        print('does not exist nope!')
                        return False
        else:
            with open('info_user.csv', 'a') as f:
                writer = csv.writer(f, delimiter=',')
                writer.writerow(username)
                print(username, 'added as new user')
                f.close()


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


if __name__ == '__main__':
    Master()