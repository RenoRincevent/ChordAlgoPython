#!/usr/bin/env python3
# -*- coding: Utf-8 -*-

import json
import threading
import os
import sys
import logging
import random
import socket

IP_MASTER = "127.0.0.1"
PORT_MASTER = 10001
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
    _data = dict() # données de chaque node
    IP_MASTER = None #ip du premier node dans le cercle
    PORT_MASTER = None #port du premier node dans le cercle
    _ip_wait = None
    _port_wait = None

    #initialise le node
    def __init__(self,ip,port,IP_MASTER,PORT_MASTER):
        self._ip = ip
        self._port = port
        self.IP_MASTER = IP_MASTER
        self.PORT_MASTER = PORT_MASTER

    def get_port(self):
        return self._port

    #indique a m1 qu'il veut s'inserer dans le cercle et lui envoie son ip et son port
    def join(self):
        jsonFrame = {}
        jsonFrame["type"] = "join"
        jsonFrame["ip"] = self._ip
        jsonFrame["port"] = self._port
        send_message(self.IP_MASTER,self.PORT_MASTER,jsonFrame)
        return

    #sur un join m1 tire au hasard une clé et vérifie qu'elle soit disponible en envoyant un lookup de cette clé
    def join_action(self,ip,port):
        key = random.randint(0, KEY_MAX)
        self._ip_wait = ip
        self._port_wait = port
        self.lookup(key,self._id)
        return

    #envoie du frame de lookup
    def lookup(self,key,id_s):
        if self._next == None: #Si aucun suivant pas besoin d'envoyer de message de lookup
            self.lookup_action(key,id_s)
            return
        jsonFrame = {}
        jsonFrame["type"] = "lookup"
        jsonFrame["key"] = int(key)
        jsonFrame["id_s"] = id_s
        ip = self._next[0]
        port = self._next[1]
        send_message(ip,port,jsonFrame)
        return

    #Sur la réception du message lookup, vérifie si la clé est disponible ou non et envoie un ans_lookup avec OK si disponible NOK si non disponible
    def lookup_action(self,key,id_s):
        print("lookup action : id thread={} key={}, id_s={}".format(self._id,key,id_s))
        if self._key == key: #si la clé est egale a la self.key alors ans_lookup NOK
            self.ans_lookup(key, "NOK", None, None, id_s[0], id_s[1])
        elif self._prev is None: #Si il n"y a qu"un seul noeud dans le cercle et que la clé disponible
            self.ans_lookup(int(key),"OK",self._id,self._id,id_s[0],id_s[1])
        elif key > self._key and key < self._prev[2]: #si la valeur de key ne fait pas partie de l"intervalle de ce node alors on lookup sur le suivant
            self.lookup(key,id_s)
        elif key < self._key or key > self._prev[2]: #si la valeur de la clé fait partie de l"intervalle de ce node et que que ce n"est pas sa clé alors ans_lookup OK
            self.ans_lookup(int(key), "OK", self._prev, self._id, id_s[0], id_s[1])
        return

    #répond au node qui a envoyer le lookup en lui disant si oui ou non la clé est disponible
    def ans_lookup(self,key,status,mb_prec,mb_next,ip,port):
        jsonFrame = {}
        jsonFrame["type"] = "ans_lookup"
        jsonFrame["key"] = int(key)
        jsonFrame["value"] = status
        jsonFrame["mbnext"] = mb_next
        jsonFrame["mbprec"] = mb_prec
        send_message(ip,port,jsonFrame)
        return

    def ans_lookup_action(self,key,status,mb_prec,mb_next):
        if(status == "OK"): #si ok on envoie une réponse au noeud pour qu'il puisse rejoindre le cercle
            self.ans_join(int(key),mb_prec,mb_next)
        else: #si not ok on re tire aléatoirement un clé est on effectue le meme processus à partir du lookup
            self.join_action(ip, port)

    #envoie de la réponse pour rejoinde le cercle
    def ans_join(self,key,mb_prec,mb_next):
        jsonFrame = {}
        jsonFrame["type"] = "ans_join"
        jsonFrame["key"] = int(key)
        jsonFrame["mbprec"] = mb_prec
        jsonFrame["mbnext"] = mb_next
        send_message(self._ip_wait,self._port_wait,jsonFrame)
        return

    #reponse pour rejoindre le cercle, met ses paramètres à jours
    def ans_join_action(self,key,mb_prec,mb_next):
        self._prev = (str(mb_prec[0]),int(mb_prec[1]),int(mb_prec[2]))
        self._next = (str(mb_next[0]),int(mb_next[1]),int(mb_next[2]))
        self._id = (str(self._ip),int(self._port),int(key))
        self.get_data()
        self.set_next()
        return

    #demander a mbnext la partie de la table de routage qui doit desormer lui appartenir
    def get_data(self):
        jsonFrame = {}
        jsonFrame["type"] = "get_data"
        jsonFrame["id_s"] = self._id
        send_message(self._next[0],self._next[1],jsonFrame)
        return

    #met mbnext a jour et envoie les nouvelle donnée a n
    def get_data_action(self,id_s):
        self._prev = (str(id_s[0]),int(id_s[1]),int(id_s[2]))
        key_prev = int(id_s[2])
        data_for_prev = dict()
        for k in self._data.keys():
            if (k <= key_prev):
                data_for_prev[int(k)] = self._data[k]
                del self._data[k]
        self.ans_get_data(self._prev,data_for_prev)
        return

    #envoie des nouvelles données a n
    def ans_get_data(self,id_s,data):
        jsonFrame = {}
        jsonFrame["type"] = "ans_get_data"
        jsonFrame["data"] = data
        send_message(id_s[0],id_s[1],jsonFrame)
        return

    #n met a jours ses nouvelles données
    def ans_get_data_action(self,data):
        for k in data:
            self._data[k] = data[k]
        return

    #indiquer a mbprec que n est desormer son suivant
    def set_next(self):
        jsonFrame = {}
        jsonFrame["type"] = "set_next"
        jsonFrame["id_n"] = self._id
        send_message(self._prev[0],self._prev[1],jsonFrame)
        return

    #mbprec met à jour son suivant
    def set_next_action(self,id_n):
        self._next = (id_n[0],id_n[1],id_n[2])
        return

    #n veut se deconnecter, il le signale à son suivant
    def disconnect(self):
        jsonFrame = {}
        jsonFrame["type"] = "disconnect"
        jsonFrame["mbprec"] = self._prev
        jsonFrame["data"] = self._data
        send_message(self._next[0],self._next[1],jsonFrame)
        return

    #le suivant met a jour sa table et son nouveau prec est le prec du noeud qui vient de se decconecter
    def disconnect_action(self,data,mb_prec):
        self._prev = (mb_prec[0],mb_prec[1],mb_prec[2])
        for k in data:
            self._data[k] = data[k]
        self.set_next()

    #envoie d'un message pour connaitre le détenteur d"une clé spécifique
    def get(self, key, id_s):
        jsonFrame = {}
        jsonFrame["type"] = "get"
        jsonFrame["key"] = int(key)
        jsonFrame["id"] = id_s
        send_message(self._next[0],self._next[1],jsonFrame)
        return

    #regarde si la clé appartient au noeud, sinon demande au suivant
    def get_action(self, key, id_s):
        if key in self._data.keys(): #la clé se situe dans la table du noeud
            self.ans_get(id_s[0],id_s[1],self.data[key],key)
        elif key < self._key or key > self._prev[2]: #si la clé n"existe pas
            self.ans_get(id_s[0],id_s[1],None,key)
        else: #On passe au suivant
            self.get(key,id_s)
        return

    #envoie de la valeur correspondant à la clé trouvé
    def ans_get(self, ip, port, value, key):
        jsonFrame = {}
        jsonFrame["type"] = "ans_get"
        jsonFrame["key"] = int(key)
        jsonFrame["value"] = value
        send_message(ip,port,jsonFrame)
        return

    #envoie un message pour inserer une valeur
    def put(self, key, value):
        jsonFrame = {}
        jsonFrame["type"] = "put"
        jsonFrame["key"] = int(key)
        jsonFrame["value"] = value
        send_message(self._next[0],self._next[1],jsonFrame)
        return

    #insère la valeur dans le bon noeud
    def put_action(self, key, value):
        if key < self._key or key > self._prev[2]: #on est dans le bon noeud => on insère la valeur
            self._data[key] = value
        else: #on n'est pas dans le bon noeud => on passe au noeud suivant
            self.put(key, value)
        return

    def listen(self, msg):
        type_message = msg["type"]
        if type_message == "join":
            self.join_action(msg["ip"],msg["port"])
        elif type_message == "lookup":
            self.lookup_action(msg["key"], msg["id_s"])
        elif type_message == "ans_lookup":
            self.ans_lookup_action(msg["key"],msg["value"],msg["mbprec"],msg["mbnext"])
        elif type_message == "ans_join":
            self.ans_join_action(msg["key"],msg["mbprec"],msg["mbnext"])
        elif type_message == "get_data":
            self.get_data_action(msg["id_s"])
        elif type_message == "ans_get_data":
            self.ans_get_data_action(msg["data"])
        elif type_message == "set_next":
            self.set_next_action(msg["id_n"])
        elif type_message == "disconnect":
            self.disconnect_action(msg["mbprec"], msg["data"])
        elif type_message = "get":
            self.get_action(msg["key"],msg["id"])
        elif type_message == "put":
            self.put_action(msg["key"],msg["value"])
        else:
            print(" {} /!\ bad message type /!\ ".format(type_message))
