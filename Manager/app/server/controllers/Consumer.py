from ..models import *

class Consumer:
    id = 0
    def __init__(self):
        self.__id = Consumer.id
        self.__topic = None
        Consumer.id += 1
        
    async def save(self, database):
        if database is not None:
            await database.create(ConsumerModel, id=self.__id)
    
    def getId(self):
        return self.__id
    
    def setTopic(self, topic):
        self.__topic = topic
    
    def getTopic(self):
        return self.__topic

    def modelToObj(instance, database):
        consumer = Consumer()
        consumer.__id = instance.id
        return consumer