from .Producer import Producer
from .Consumer import Consumer
from .Partition import Partition
from .utils import RoundRobin

from ..models import *

class Topic:
    def __init__(self, topic_name: str):
        self.__id = None
        self.__name = topic_name
        self.__producers = {}
        self.__consumers = {}
        self.__partitions = {}
        self.__consumerSelfSelector = RoundRobin()
        self.__producerSelfSelector = RoundRobin()
    
    def getId(self):
        return self.__id
    
    def getName(self):
        return self.__name
    
    async def save(self, database):
        if database is not None:
            instance = await database.create(TopicModel, name=self.__name)
            self.__id = instance.id
    
    def addProducer(self, producer: Producer):
        self.__producers[producer.getId()] = producer
        
    def addConsumer(self, consumer: Consumer):
        self.__consumers[consumer.getId()] = consumer
    
    def removeProducer(self, id: int):
        if self.__producers.get(id) is None:
            return
        del self.__producers[id]
        
    def removeConsumer(self, id: int):
        if self.__consumers.get(id) is None:
            return
        del self.__consumers[id]
        
    def addPartition(self, partition: Partition):
        self.__partitions[partition.getId()] = partition
        
    def removePartition(self, partition: Partition):
        del self.__partitions[partition.getId()]
        
    def getPartitions(self) -> list:
        return list(self.__partitions.values())

    def selfSelect(self, producer = True) -> Partition:
        if producer:
            return self.__producerSelfSelector.generate(list(self.__partitions.values()))
        else:
            return self.__consumerSelfSelector.generate(list(self.__partitions.values()))
        
    def modelToObj(instance, database):
        topic = Topic(instance.name)
        topic.__id = instance.id
        return topic
            