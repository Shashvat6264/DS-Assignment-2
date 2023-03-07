from .Message import Message
from .Partition import Partition
from .exceptions import *

from ..models import *

class DistributedQueue:
    def __init__(self, database = None):
        self.__database = database
        self.__partitions = {}

    def loadPersistence(self):
        if self.__database is not None:
            partitionList = self.__database.read(PartitionModel)
            for partitionInstance in partitionList:
                self.__partitions[partitionInstance.id] = Partition.modelToObj(partitionInstance, self.__database)
    
    def getPartitions(self) -> list:
        return list(self.__partitions.values())
    
    async def createPartition(self, topic_name: str, partition_id: int):
        if partition_id in self.__partitions.keys():
            raise PartitionAlreadyExists("Partition of id: {partition_id} already exists")
        partition = Partition(partition_id, topic_name, self.__database)
        self.__partitions[partition_id] = partition
        await partition.save()
    
    async def enqueue(self, partition_id: int, id: int, message: str):
        if self.__partitions.get(partition_id) is None:
            raise PartitionDoesNotExist("Partition id: {partition_id} does not exist")
        msg = Message(message)
        await msg.save(self.__database)
        await self.__partitions[partition_id].pushMessage(id, msg)
        
    async def dequeue(self, partition_id: int, id: int) -> str:
        if self.__partitions.get(partition_id) is None:
            raise PartitionDoesNotExist("Partition id: {partition_id} does not exist")
        message = await self.__partitions[partition_id].popMessage(id)
        return message.getMessage()
    
    async def size(self, partition_id: int, id: int) -> int:
        if self.__partitions.get(partition_id) is None:
            raise PartitionDoesNotExist("Partition id: {partition_id} does not exist")
        return await self.__partitions[partition_id].getSize(id)
        
    
    