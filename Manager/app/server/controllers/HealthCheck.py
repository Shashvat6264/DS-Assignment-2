from enum import Enum
import time
import threading
import os
import asyncio

class HeartBeatType(Enum):
    CONSUMER = 0
    PRODUCER = 1
    BROKER = 2

class HeartBeat:
    def __init__(self, address, heartbeatType: HeartBeatType, key):
        self.address = address
        self.type = heartbeatType
        self.time = time.time()
        self.key = key
    

class HealthCheck:
    def __init__(self, manager):
        self.__manager = manager
        self.__beat = [{}, {}, {}]
        manager.setHealthManager(self)
    
    def heartbeat(self, heartbeat: HeartBeat):
        if self.__beat[int(heartbeat.type)].get(heartbeat.key) is not None:
            oldbeat, oldtimer = self.__beat[int(heartbeat.type)][heartbeat.key]
            oldtimer.cancel()
        newtimer = threading.Timer(60, self.ping(heartbeat))
        newtimer.start()
        self.__beat[int(heartbeat.type)][heartbeat.key] = (heartbeat, newtimer)
        
    def ping(self, heartbeat: HeartBeat):
        def wrapper():
            """Host is down"""
            if int(heartbeat.type) == 2:
                asyncio.run(self.__manager.removeBroker(self.__manager.getBroker(heartbeat.key)))
            
            if int(heartbeat.type) == 1:
                asyncio.run(self.__manager.removeConsumer(heartbeat.key))
                
            if int(heartbeat.type) == 0:
                asyncio.run(self.__manager.removeProducer(heartbeat.key))
                
        return wrapper