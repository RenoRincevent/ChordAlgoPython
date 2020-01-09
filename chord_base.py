#!/usr/bin/python3
# -*- coding: Utf-8 -*-

import threading
import json
import socket
import logging
import signal
import sys
import time
from chord import Node

BUFFER_SIZE = 2048
node = None

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

#@threaded
def receive():
    PORT_BASE = node.get_port()
    s      = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(('', PORT_BASE))
    except socket.error as e:
        try:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind(('', PORT_BASE))
        except socket.error as e:
            s.close()
            log.error("Node %d - bind: %s\n" %(key, e))
            return
    s.listen(2)
    while True:
        try:
            print("just here")
            client, addr = s.accept()
            print("just here2")
        except socket.error:
            break
        rec = ''
        allReceived = False
        try:
            while(not allReceived):
                incomingData = client.recv(BUFFER_SIZE).decode()
                if(incomingData == ''):
                    allReceived = True
                else:
                    rec += incomingData
        except socket.error as e:
            log.error("Node %d - recv: %s\n" % (key, e))
            s.close()
            return
        try:
            msg = json.loads(rec)
            print(msg)
        except Exception as e:
            log.error("Node %d - json.loads: %s\n" % (key, e))
            return
        node.listen(msg)
        print("Etat du node ID={}, Prev={}, Next={}, Data={}\n######".format(node._id,node._prev,node._next,node._data))

def signal_handler(sig, frame):
   print("Catch Ctrl-C")
   sys.exit(0)


def main(argv):
    signal.signal(signal.SIGINT, signal_handler)
    global node
    print("TEST")
    node = Node(argv[1],int(argv[2]),argv[3],int(argv[4]))
    receive()
    if argv[1] == argv[3] and argv[2] == argv[4]: #on cree le master
        node._id = (argv[1],int(argv[2]),1)
        node._key = 1
    else:
        node.join()

if __name__ == "__main__":
    argv = sys.argv[:]
    main(argv)
