from ..models import *

class Producer:
    id = 0
    def __init__(self):
        self.__id = Producer.id
        self.__topic = None
        Producer.id += 1
        
    async def save(self, database):
        if database is not None:
            await database.create(ProducerModel, id=self.__id)
    
    def getId(self):
        return self.__id
    
    def setTopic(self, topic):
        self.__topic = topic
    
    def getTopic(self):
        return self.__topic
    
    def modelToObj(instance, database):
        producer = Producer()
        producer.__id = instance.id
        return producer