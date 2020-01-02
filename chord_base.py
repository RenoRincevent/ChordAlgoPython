#!/usr/bin/python3
# -*- coding: Utf-8 -*-

import threading
import json
import socket
import logging
import signal
from chord import Node

BUFFER_SIZE = 2048

IP_NODE = "127.0.0.1"
PORT_NODE = 10005
node = None

def threaded(fn):
    def wrapper(*args, **kwargs):
        thread = threading.Thread(target=fn, args=args, kwargs=kwargs)
        thread.start()
        return thread
    return wrapper

@threaded
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
            client, addr = s.accept()
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
        node.on_message(msg)

#def signal_handler(sig, frame):
#    print("Catch Ctrl-C")
#    sys.exit(0)


def main():
#    signal.signal(signal.SIGINT, signal_handler)
    print("TEST")
    node = Node(IP_NODE,PORT_NODE)
    node.join()
    receive()

if __name__ == "__main__":
	main()
