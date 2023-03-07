from .Partition import Partition
import requests
from .exceptions import *

from ..models import *

class Broker:
    def __init__(self, address):
        self.__id = None
        self.__address = address
        
    def getId(self):
        return self.__id
    
    def getAddress(self):
        return self.__address
    
    async def save(self, database):
        if database is not None:
            instance = await database.create(BrokerModel, address=self.__address)
            self.__id = instance.id
    
    async def createPartition(self, partition: Partition):
        """
        Requesting broker to create partition
        """
        try:    
            response = requests.post(
                self.__address + '/partitions',
                json = {
                    'topic_name': partition.getTopic().getName(),
                    'partition_id': partition.getId()
                }
            )
        except requests.ConnectionError as e:
            raise BrokerDownError(self, "The broker is down")
        
        if response.status_code != 201 and response.status_code != 404:
            raise BrokerError(response.json()['message'])
        elif response.status_code == 404:
            raise BrokerError(self, response.json()['message'])
        
    async def getSize(self, partition_id: int, id: int) -> int:
        """
        Requesting size of partition from broker
        """
        try:
            response = requests.get(
                self.__address + '/size',
                params = {
                    'partition_id': partition_id,
                    'consumer_id': id
                }
            )
        except requests.ConnectionError as e:
            raise BrokerDownError(self, "The broker is down")
        
        if response.status_code == 200:
            return response.json()['size']
        elif response.status_code == 404:
            raise BrokerError(self, response.json()['message'])
        else:
            raise BrokerError(self.__address, response.json()['message'])
        
    async def enqueue(self, partition: Partition, message: str, id: int):
        """
        Requesting broker to push a message in the partition queue
        """
        try:    
            response = requests.post(
                self.__address + '/producer/produce',
                json = {
                    'partition_id': partition.getId(),
                    'producer_id': id,
                    'message': message
                }
            )
        except requests.ConnectionError as e:
            raise BrokerDownError(self, "The broker is down")
        
        if response.status_code != 200 and response.status_code != 404:
            raise BrokerError(self, response.json()['message'])
        elif response.status_code == 404:
            raise BrokerError(self.__address, response.json()['message'])
    
    async def dequeue(self, partition: Partition, id: int) -> str:
        """
        Requesting broker to pop a message from the queue for a consumer
        """
        try:
            response = requests.get(
                self.__address + '/consumer/consume',
                params = {
                    'partition_id': partition.getId(),
                    'consumer_id': id
                }
            )
        except requests.ConnectionError as e:
            raise BrokerDownError(self, "The broker is down")
        
        if response.status_code != 200 and (response.status_code != 400 or response.json()['message'] != "QEmpty"):
            raise BrokerError(self, response.json()['message'])
        elif response.status_code == 400 and response.json()['message'] == "QEmpty":
            raise QueueEmpty("Queue is empty")
        elif response.status_code == 200:
            return response.json()['message']
        
    def modelToObj(instance, database):
        broker = Broker(instance.address)
        broker.__id = instance.id
        return broker