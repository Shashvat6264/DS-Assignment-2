from ..Partition import Partition
from .exceptions import *

class PartitionMechanism:
    def getPartition(self) -> int:
        pass
    
    class Meta:
        abstract = True


class SimpleHashingMechanism(PartitionMechanism):
    def getPartition(self, partition_key: int, topic) -> Partition:
        partitions = topic.getPartitions()
        size = len(partitions)
        if size == 0:
            raise EmptyList("Partition list is empty")
        index = partition_key%size
        return partitions[index]
        