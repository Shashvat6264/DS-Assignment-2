from ..models import *

class Partition:
    id = 0
    def __init__(self, topic, broker):
        self.__topic = topic
        self.__id = Partition.id
        self.__broker = broker
        Partition.id += 1
        
    async def save(self, database):
        if database is not None:
            await database.create(PartitionModel, id=self.__id, broker_id=self.__broker.getId(), topic_id=self.__topic.getId())
        
    def getBroker(self):
        return self.__broker
        
    def getId(self):
        return self.__id
    
    def getTopic(self):
        return self.__topic
    
    def setTopic(self, topic):
        self.__topic = topic
        
    def setBroker(self, broker):
        self.__broker = broker
        
    def setId(self, id: int):
        self.__id = id
    
    def modelToObj(instance, database):
        partition = Partition(None, None)
        partition.__id = instance.id
        return partition