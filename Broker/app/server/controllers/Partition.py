from .LockedQueue import LockedQueue
from .Message import Message

from ..models import *

class Partition:
    def __init__(self, partition_id: int, topic_name: str, database = None):
        self.__id = partition_id
        self.__topic = topic_name
        self.__database = database
        self.__queue = LockedQueue()
        self.__consumerOffsets = {}
        
    def getId(self) -> int:
        return self.__id
    
    def getTopic(self) -> str:
        return self.__topic
    
    async def save(self):
        if self.__database is not None:
            await self.__database.create(PartitionModel, id=self.__id, topicname=self.__topic)
    
    async def pushMessage(self, id: int, message: Message):
        index = await self.__queue.push(message)
        if self.__database is not None:
            partitionInstance = self.__database.getById(PartitionModel, self.__id)
            messageInstance = self.__database.getById(MessageModel, message.getId())
            partitionInstance.messages.append(messageInstance)
            messageInstance.index = index
            messageInstance.partition_id = partitionInstance.id
            await self.__database.updateById(MessageModel, message.getId(), index=messageInstance.index, partition_id=messageInstance.partition_id)
            await self.__database.updateById(PartitionModel, self.__id, messages=partitionInstance.messages)
        
    async def popMessage(self, id: int) -> Message:
        if self.__consumerOffsets.get(id) is None:
            self.__consumerOffsets[id] = 0
        
            if self.__database is not None:
                consumerInstance = await self.__database.create(ConsumerModel, pid=id, offset=0, partition_id=self.__id)
                partitionInstance = self.__database.getById(PartitionModel, self.__id)
                partitionInstance.consumers.append(consumerInstance)
                await self.__database.updateById(PartitionModel, self.__id, consumers=partitionInstance.consumers)
        
        message = await self.__queue.getIndex(self.__consumerOffsets[id])
        self.__consumerOffsets[id] += 1
        
        if self.__database is not None:
            partitionInstance = self.__database.getById(PartitionModel, self.__id)
            for consumerInstance in partitionInstance.consumers:
                if consumerInstance.pid == id:
                    await self.__database.updateById(ConsumerModel, consumerInstance.id, offset=consumerInstance.offset+1)
                    break
        return message
        
    async def getSize(self, id: int) -> int:
        if self.__consumerOffsets.get(id) is None:
            self.__consumerOffsets[id] = 0
            
            if self.__database is not None:
                consumerInstance = await self.__database.create(ConsumerModel, pid=id, offset=0, partition_id=self.__id)
                partitionInstance = self.__database.getById(PartitionModel, self.__id)
                partitionInstance.consumers.append(consumerInstance)
                await self.__database.updateById(PartitionModel, self.__id, consumers=partitionInstance.consumers)
                
        total_size = await self.__queue.size()
        return total_size - self.__consumerOffsets[id]
    
    def modelToObj(instance, database):
        partition = Partition(partition_id=instance.id, topic_name=instance.topicname, database=database)
        sorted(instance.messages, key=lambda x: x.index)
        messageList = []
        for messageInstance in instance.messages:
            message = Message.modelToObj(messageInstance)
            messageList.append(message)
        
        partition.__queue.inject(messageList)
        
        for consumerInstance in instance.consumers:
            partition.__consumerOffsets[consumerInstance.pid] = consumerInstance.offset
        
        return partition