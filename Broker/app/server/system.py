from .controllers import *

from .database import get_db, engine

from . import models

models.Base.metadata.create_all(bind=engine)

main_dq = DistributedQueue(database=get_db())
main_dq.loadPersistence()

"""
    Service discovery mechanism -> Broker sends request to manager notifying that the broker is available
"""

import requests
import os
MANAGER_ADDRESS = os.getenv('MANAGER_ADDRESS')
if MANAGER_ADDRESS != None:
    CURRENT_ADDRESS = os.getenv('CURRENT_ADDRESS')
    if CURRENT_ADDRESS != None:
        partitions = main_dq.getPartitions()
        partitionInstances = []
        for partition in partitions:
            partitionInstances.append({
                'id': partition.getId(),
                'topic': partition.getTopic()
            })
        response = requests.post(
            MANAGER_ADDRESS + '/brokers',
            json={
                'broker_address': CURRENT_ADDRESS,
                'partitions': partitionInstances
            }
        )

        if response.status_code != 201 and response.status_code != 208:
            raise ManagerNotFound(f"Broker Manager not found on {MANAGER_ADDRESS}")
    else:
        raise ManagerNotFound("Current Address not set in environment")
else:
    raise ManagerNotFound("Manager Address not set in environment")

"""
    Sending out heartbeats at 30s intervals
"""

import threading
import time

class HeartBeatThread(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        
    def run(self):
        while (1):
            time.sleep(15)
            try:
                requests.get(MANAGER_ADDRESS + '/health', params={'type': 2, 'key': CURRENT_ADDRESS})
            except requests.ConnectionError as e:
                raise ManagerNotFound(f"Broker Manager not found on {MANAGER_ADDRESS}")
            
heartbeatThread = HeartBeatThread()
heartbeatThread.start()



