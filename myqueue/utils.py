def convertToList(function):
    def wrapper(*args, **kwargs):
        notlist = not isinstance(args[1], list)
        if notlist:
            l = list(args)
            l[1] = [l[1]]
            args = tuple(l)
        response = function(*args, **kwargs)
        if response is not None:
            if notlist:
                return response[0]
            return response
    return wrapper

import threading
import time
import requests

from .exceptions import *

class HeartBeatThread(threading.Thread):
    def __init__(self, address, id):
        threading.Thread.__init__(self)
        self.__address = address
        self.__id = id
        
    def run(self):
        while (1):
            time.sleep(15)
            try:
                requests.get(self.__address + '/health', params={'type': 2, 'key': self.__id})
            except requests.ConnectionError as e:
                raise ManagerNotFound(f"Broker Manager not found on {self.__address}")