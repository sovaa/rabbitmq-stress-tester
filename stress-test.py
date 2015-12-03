#!/bin/python

import threading
from time import sleep
import pika
import time
import argparse
import math
import sys

payload = "{'jid':'foo@xmpp.tld','body':{'event_name':'xmpp:refresh_inbox','state':{'inbox_messages':4}}}"


def parse_arguments():
    parser = argparse.ArgumentParser()
    parser.add_argument('--payload', type=str, default=payload, help="the payload of the message to send")
    parser.add_argument('--username', type=str, default='guest', help="the username to use (default: guest)")
    parser.add_argument('--password', type=str, default='guest', help="the password to use (default: guest)")
    parser.add_argument('--host', type=str, default='localhost', help="the hostname of the server (default: localhost)")
    parser.add_argument('--port', type=int, default='5672', help="the server port (default: 5672)")
    parser.add_argument('--threads', type=int, default='2', help="number of producer threads to start (default: 2)")
    parser.add_argument('--messages', type=int, default='6', help="number of messages each thread should send (default: 6)")
    parser.add_argument('--vhost', type=str, default='/', help="the virtual host (default: /)")
    parser.add_argument('--exchange', type=str, default='amq.direct', help="the exchange (default: amq.direct)")
    parser.add_argument('--key', type=str, default='', help="the routing key (default is blank key)")
    return parser.parse_args()


def create_connection_params(args):
    credentials = pika.PlainCredentials(args.username, args.password)
    return pika.ConnectionParameters(args.host, args.port, args.vhost, credentials)


def tprint(msg):
    name = threading.current_thread().name
    spaces = ' '*(11-len(name))
    print >>sys.stdout, "[" + threading.current_thread().name + "]" + spaces + msg + "\n",


def publisher(args, connection_params):
    connection = pika.BlockingConnection(connection_params)
    channel = connection.channel()

    progress = 0
    for i in range(args.messages):
        progress = math.floor(((i+1)*1.0 / args.messages)*100)
        if progress % 20 == 0:
            tprint(str(progress) + "% done")
            progress = progress + 1
        channel.basic_publish(args.exchange, args.key, args.payload)

    channel.close()
    connection.close()
    tprint("finished, terminating thread")


def main(args):
    connection_params = create_connection_params(args)
    print "starting", args.threads, "threads sending", args.messages, "messages each, totalling", \
        (args.threads*args.messages), "messages"

    start_time = time.time()
    threads = []

    for i in range(args.threads):
        thread = threading.Thread(target = publisher, args = (args, connection_params, ))
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()

    print "--- %s seconds ---" % (time.time() - start_time)
    print "all threads finished... exiting"


if __name__ == "__main__":
    args = parse_arguments()
    main(args)

