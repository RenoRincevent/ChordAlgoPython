#!/usr/bin/env python3
# -*- coding: Utf-8 -*-
import json
import threading
import os
import sys
import logging
import random
import socket

IP_NODE = "127.0.0.1"
PORT_NODE = 10005
KEY_MAX = 255

def send_message(ip,port,jsonFrame):
    s = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    s.connect((ip,port))
    s.send(json.dumps(jsonFrame).encode())
    s.close()
    return

class Node(object):

    #class attributes
    _ip = None
    _port = None
    _key = None
    _id = None #id = (ip,port,key)
    _prev = None # id du précédent
    _next = None # id du suivant

    #initialise le node
    def __init__(self,ip,port):
        self._ip = ip
        self._port = port

    def get_port(self):
        return self._port

    #indique a m1 qu'il veut s'inserer dans le cercle et lui envoie son ip et son port
    def join(self):
        jsonFrame = {}
        jsonFrame['type'] = 'join'
        jsonFrame['ip'] = self._ip
        jsonFrame['port'] = self._port
        send_message(self._ip,self._port,jsonFrame)
        return

    #sur un join m1 tire au hasard une clé et vérifie qu'elle soit disponible en envoyant un lookup de cette clé
    def on_join(self,ip,port):
        key = random.randint(0, KEY_MAX)
        self.lookup(key,self._id)
        return

    #envoie du frame de lookup
    def lookup(self,key,id_s):
        jsonFrame = {}
        jsonFrame['type'] = 'lookup'
        jsonFrame['key'] = int(key)
        jsonFrame['id_s'] = id_s
        ip = self._next[0]
        port = self._next[1]
        send_message(ip,port,jsonFrame)
        return

    #Sur la réception du message lookup, vérifie si la clé est disponible ou non et envoie un ans_lookup avec OK si disponible NOK si non disponible
    def on_lookup(self,key,id_s):
        if self._key == key: #si la clé est egale a la self.key alors ans_lookup NOK
            self.ans_lookup(key, 'NOK', None, None, id_s[0], id_s[1])
        elif key > self._key: #si la valeur de key ne fait pas partie de l'intervelle de ce node alors on lookup sur le suivant
            self.lookup(key,id_s)
        elif key < self._key and key > self._prev[2]: #si la valeur de la clé fait partie de l'intervalle de ce node et que que ce n'est pas sa clé alors ans_lookup OK
            self.ans_lookup(int(key), 'OK', self._id, self._prev, id_s[0], id_s[1])
        return

    #répond au node qui a envoyer le lookup en lui disant si oui ou non la clé est disponible
    def ans_lookup(self,key,status,mb_prec,mb_next,ip,port):
        jsonFrame = {}
        jsonFrame['type'] = 'ans_lookup'
        jsonFrame['key'] = int(key)
        jsonFrame['value'] = status
        jsonFrame['mbnext'] = mb_next
        jsonFrame['mbprec'] = mb_prec
        send_message(ip,port,jsonFrame)
        return

    def on_ans_lookup(self,key,status,mb_prec,mb_next):
        if(status == 'OK'): #si ok on envoie une réponse au noeud pour qu'il puisse rejoindre le cercle
            self.ans_join(int(key),mb_prec,mb_next)
        else: #si not ok on re tire aléatoirement un clé est on effectue le meme processus à partir du lookup
            self.on_join(ip, port)

    #envoie de la réponse pour rejoinde le cercle
    def ans_join(self,key,mb_prec,mb_next):
        jsonFrame = {}
        jsonFrame['type'] = 'ans_join'
        jsonFrame['key'] = int(key)
        jsonFrame['mbprec'] = mb_prec
        jsonFrame['mbnext'] = mb_next
        send_message(self._ip,self._port,jsonFrame)
        return

    def on_message(self, msg):
        type_message = msg['type']
        if type_message == "join":
            ip = msg["ip"]
            port = msg["port"]
            self.on_join(ip, port)
        elif type_message == "lookup":
            key = msg["key"]
            id_s = msg["id_s"]
            self.on_lookup(key, id_s)
        elif type_message == "ans_lookup":
            key = msg["key"]
            status = msg["value"]
            mb_prec = msg["mbprec"]
            mn_next = msg["mbnext"]
            on_ans_lookup(key,status,mb_prec,mb_next)
        elif type_message == "ans_join": #reponse pour rejoindre le cercle, met ses paramètre a jours
            self._key = int(msg['key'])
            mbprec = msg['mbprec']
            mbnext = msg['mbnext']
            self._prev = (str(mbprec[0]), int(mbprec[1]), int(mbprec[2]))
            self._next = (str(mbnext[0]), int(mbnext[1]), int(mbnext[2]))
            self._id = (str(self._ip), int(self._port), int(self._key))
            # self.get_data() #demander a mbnext la partie de la table de routage qui doit desormer lui appartenir et se mettre a jour
            # self.set_next() #indiquer a mbprec que n est desormer son suivant
        else:
            print("Unknown message type %s\n" % msgtype)


    #def on_join
    #def lookup(key, id_s):

def main():
    print("TEST")
    n1 = Node(IP_NODE,PORT_NODE)
    n1.join()

if __name__ == "__main__":
	main()
