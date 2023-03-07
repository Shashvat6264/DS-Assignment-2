from .Topic import Topic
from .Partition import Partition
from .Producer import Producer
from .Consumer import Consumer
from .Broker import Broker
from .exceptions import *
from .utils import *
from .HealthCheck import *
import random

from ..models import *
import os 
READ_ONLY = os.getenv('READ_ONLY')

class BrokerManager:
    def __init__(self, database = None):
        self.__database = database
        self.__healthManager = None
        self.__brokers = {}
        self.__producers = {}
        self.__consumers = {}
        self.__topics = {}
        self.__partitions = {}
        self.__partitionMechanism = SimpleHashingMechanism()
        
    def setHealthManager(self, manager):
        self.__healthManager = manager
        
    def loadPersistence(self):
        if self.__database is not None:
            topicList = self.__database.read(TopicModel)
            for topicInstance in topicList:
                topic = Topic.modelToObj(topicInstance, self.__database)
                self.__topics[topic.getName()] = topic
            
            brokerList = self.__database.read(BrokerModel)
            for brokerInstance in brokerList:
                broker = Broker.modelToObj(brokerInstance, self.__database)
                self.__brokers[broker.getAddress()] = broker
            
            consumerList = self.__database.read(ConsumerModel)
            for consumerInstance in consumerList:
                consumer = Consumer.modelToObj(consumerInstance, self.__database)
                topic = self.__topics[consumerInstance.topic.name]
                consumer.setTopic(topic)
                topic.addConsumer(consumer)
                self.__consumers[consumer.getId()] = consumer
                
            producerList = self.__database.read(ProducerModel)
            for producerInstance in producerList:
                producer = Producer.modelToObj(producerInstance, self.__database)
                topic = self.__topics[producerInstance.topic.name]
                producer.setTopic(topic)
                topic.addProducer(producer)
                self.__producers[producer.getId()] = producer
            
            partitionList = self.__database.read(PartitionModel)
            for partitionInstance in partitionList:
                if self.__brokers.get(partitionInstance.broker.address) is not None:
                    partition = Partition.modelToObj(partitionInstance, self.__database)
                    partition.setTopic(self.__topics[partitionInstance.topic.name])
                    partition.setBroker(self.__brokers[partitionInstance.broker.address])
                    self.__partitions[partition.getId()] = partition
                    self.__topics[partitionInstance.topic.name].addPartition(partition)
            
    
    async def addBroker(self, broker_address: str, partitions: list = None):
        if self.__brokers.get(broker_address) is not None:
            raise BrokerAlreadyPresent(f"Broker {broker_address} is already present")
        self.__brokers[broker_address] = Broker(broker_address)
        await self.__brokers[broker_address].save(self.__database)
        
        if self.__healthManager is not None:
            self.__healthManager.heartbeat(HeartBeat(None, 2, broker_address))
        
        if partitions is not None:
            for partitionInstance in partitions:
                if self.__partitions.get(partitionInstance['id']) is None:
                    if self.__topics.get(partitionInstance['topic']) is None:
                        self.__topics[partitionInstance['topic']] = Topic(partitionInstance['topic'])
                        await self.__topics[partitionInstance['topic']].save(self.__database)
                    topic = self.__topics[partitionInstance['topic']]
                    broker = self.__brokers[broker_address]
                    partition = Partition(topic, broker)
                    partition.setId(partitionInstance['id'])
                    self.__partitions[partitionInstance['id']] = partition
                    topic.addPartition(partition)
                    await partition.save(self.__database)
                
    
    async def removeBroker(self, broker: Broker):
        partitions = list(self.__partitions.values())
        for partition in partitions:
            if partition.getBroker() == broker:
                topic = partition.getTopic()
                topic.removePartition(partition)
                if self.__database is not None:
                    await self.__database.deleteById(PartitionModel, partition.getId())
                del self.__partitions[partition.getId()]
        if self.__database is not None:
            await self.__database.deleteById(BrokerModel, broker.getId())
        del self.__brokers[broker.getAddress()]
        

    def getBroker(self, address: str):
        return self.__brokers.get(address)
    
    async def removeConsumer(self, id: int):
        if self.__consumers.get(id) is None:
            return
        
        for topic in self.__topics.values():
            topic.removeConsumer(id)
        
        if self.__database is not None:
            await self.__database.deleteById(ConsumerModel, id)
        del self.__consumers[id]
            
    async def removeProducer(self, id: int):
        if self.__producers.get(id) is None:
            return
        
        for topic in self.__topics.values():
            topic.removeProducer(id)
            
        if self.__database is not None:
            await self.__database.deleteById(ProducerModel, id)
        del self.__producers[id]
    
    async def listBrokers(self):
        if READ_ONLY:
            if self.__database is not None:
                brokerList = self.__database.read(BrokerModel)
                brokers = []
                for brokerInstance in brokerList:
                    if brokerInstance.address not in brokers:
                        brokers.append(brokerInstance.address)
                return brokers
            else:
                return []
        return list(self.__brokers.keys())

    async def createTopic(self, topic_name: str, number_of_partitions: int = 3):
        if self.__topics.get(topic_name) is not None:
            raise TopicAlreadyExists(f"Topic {topic_name} already exists")
        if len(self.__brokers.keys()) == 0:
            raise NoBrokerPresent("No broker connected to manager")
        """
        Design Choice:
        If user specifies number of partitions for a topic, that many partitions are created.
        Else default number of partitions that are created are 3
        """
        self.__topics[topic_name] = Topic(topic_name)
        await self.__topics[topic_name].save(self.__database)
        for _ in range(number_of_partitions):
            assigned = False
            while not assigned:
                if len(self.__brokers.keys()) == 0:
                    raise NoBrokerPresent("No broker connected to manager")
                try:
                    selected_broker = random.choice(list(self.__brokers.keys()))
                    await self.createPartition(topic_name, selected_broker)
                    assigned = True
                except BrokerDownError as e:
                    await self.removeBroker(e.broker)
        
                
    async def listTopics(self) -> list:
        if READ_ONLY:
            if self.__database is not None:
                topicList = self.__database.read(TopicModel)
                topics = []
                for topicInstance in topicList:
                    if topicInstance.name not in topics:
                        topics.append(topicInstance.name)
                return topics
            else:
                return []
        return list(self.__topics.keys())
        
    async def createPartition(self, topic_name: str, broker_address: str):
        partition = Partition(self.__topics[topic_name], self.__brokers[broker_address])
        await self.__brokers[broker_address].createPartition(partition)
        self.__partitions[partition.getId()] = partition
        self.__topics[topic_name].addPartition(partition)
    
        await partition.save(self.__database)
        if self.__database is not None:
            topicInstance = self.__database.getById(TopicModel, self.__topics[topic_name].getId())
            partitionInstance = self.__database.getById(PartitionModel, partition.getId())
            brokerInstance = self.__database.getById(BrokerModel, self.__brokers[broker_address].getId())
            topicInstance.partitions.append(partitionInstance)
            brokerInstance.partitions.append(partitionInstance)
            partitionInstance.topic_id = topicInstance.id
            partitionInstance.broker_id = brokerInstance.id
            await self.__database.updateById(TopicModel, topicInstance.id, partitions=topicInstance.partitions)
            await self.__database.updateById(PartitionModel, partition.getId(), topic_id=partitionInstance.topic_id, broker_id=partitionInstance.broker_id)
            await self.__database.updateById(BrokerModel, brokerInstance.id, partitions=brokerInstance.partitions)
    
    async def registerProducer(self, topic_name: str) -> int:
        if self.__topics.get(topic_name) is None:
            await self.createTopic(topic_name)
        
        producer = Producer()
        producer.setTopic(self.__topics[topic_name])
        self.__producers[producer.getId()] = producer
        
        await producer.save(self.__database)
        if self.__database is not None:
            topicInstance = self.__database.getById(TopicModel, self.__topics[topic_name].getId())
            producerInstance = self.__database.getById(ProducerModel, producer.getId())
            topicInstance.producers.append(producerInstance)
            producerInstance.topic_id = topicInstance.id
            await self.__database.updateById(ProducerModel, producer.getId(), topic_id=producerInstance.topic_id)
            await self.__database.updateById(TopicModel, topicInstance.id, producers=topicInstance.producers)
        
        if self.__healthManager is not None:
            self.__healthManager.heartbeat(HeartBeat(None, 1, producer.getId()))
        
        return producer.getId()
    
    async def registerConsumer(self, topic_name: str) -> int:
        if self.__topics.get(topic_name) is None:
            raise TopicDoesNotExist(f"Topic -> {topic_name} does not exist")
        
        consumer = Consumer()
        consumer.setTopic(self.__topics[topic_name])
        self.__consumers[consumer.getId()] = consumer
        
        await consumer.save(self.__database)
        if self.__database is not None:
            topicInstance = self.__database.getById(TopicModel, self.__topics[topic_name].getId())
            consumerInstance = self.__database.getById(ConsumerModel, consumer.getId())
            topicInstance.consumers.append(consumerInstance)
            consumerInstance.topic_id = topicInstance.id
            await self.__database.updateById(ConsumerModel, consumer.getId(), topic_id=consumerInstance.topic_id)
            await self.__database.updateById(TopicModel, topicInstance.id, consumers=topicInstance.consumers)
        
        if self.__healthManager is not None:
            self.__healthManager.heartbeat(HeartBeat(None, 0, consumer.getId()))
        
        return consumer.getId()
    
    def __authorizeProducer(self, topic_name: str, id: int) -> bool:
        return self.__producers[id].getTopic().getName() == topic_name
    
    def __authorizeConsumer(self, topic_name: str, id: int) -> bool:
        return self.__consumers[id].getTopic().getName() == topic_name
    
    async def enqueue(self, topic_name: str, id: int, message: str, partition_key: int = None):
        if self.__topics.get(topic_name) is None:
            raise TopicDoesNotExist(f"Topic -> {topic_name} does not exist")
        
        if not self.__authorizeProducer(topic_name, id):
            raise UnAuthorizedAccess(f"Producer {id} is not allowed to push messages to topic {topic_name}")
        
        """
        Partition to which message is enqueued is selected by two methods
        1. Round Robin method is used when no partition key is provided by the producer
        2. Hashing Algorithm to select partition from partition key in case partition key is provided by the producer
        """
        success = False
        while not success:
            try:
                if partition_key == None:
                    partition = self.__topics[topic_name].selfSelect()
                else:
                    partition = self.__partitionMechanism.getPartition(partition_key, self.__topics[topic_name])
            except EmptyList as e:
                """
                Handling the case where the topic has no partitions or brokers with partitions of topic are down
                In that case, we create 1 new partition for the topic
                """
                assigned = False
                while not assigned:
                    if len(self.__brokers.keys()) == 0:
                        raise NoBrokerPresent("No broker connected to manager")
                    try:
                        selected_broker = random.choice(list(self.__brokers.keys()))
                        await self.createPartition(topic_name, selected_broker)
                        assigned = True
                    except BrokerDownError as e:
                        await self.removeBroker(e.broker)
                continue
            try:
                await partition.getBroker().enqueue(partition, message, id)
                success = True
            except BrokerDownError as e:
                await self.removeBroker(e.broker)
            
    async def dequeue(self, topic_name: str, id: int, partition_key: int = None) -> str:
        if READ_ONLY:
            self.loadPersistence()
        
        if self.__topics.get(topic_name) is None:
            raise TopicDoesNotExist(f"Topic -> {topic_name} does not exist")
        
        if not self.__authorizeConsumer(topic_name, id):
            raise UnAuthorizedAccess(f"Consumer {id} is not allowed to pull messages from topic {topic_name}")
        
        if await self.size(topic_name, id) == 0:
            raise QueueEmpty(f"Queue for topic -> {topic_name} is empty")
        
        """
        Partition from which message is dequeued is selected by two methods
        1. Round Robin method is used when no partition key is provided by the producer
        2. Hashing Algorithm to select partition from partition key in case partition key is provided by the producer
        """    
        success = False
        while not success:
            if await self.size(topic_name, id) == 0:
                raise QueueEmpty(f"Queue for topic -> {topic_name} is empty")
            try:
                if partition_key == None:
                    partition = self.__topics[topic_name].selfSelect(producer = False)
                else:
                    partition = self.__partitionMechanism.getPartition(partition_key, self.__topics[topic_name])
            except EmptyList as e:
                """
                Handling the case where the topic has no partitions or brokers with partitions of topic are down
                In that case, we return an error
                """
                raise NoBrokerPresent(f"No Broker is currently present with partitions of topic {topic_name}")
            try:
                message = await partition.getBroker().dequeue(partition, id)
                return message
            except BrokerDownError as e:
                await self.removeBroker(e.broker)
            except QueueEmpty as e:
                if partition_key is not None:
                    raise e
        
    async def size(self, topic_name: str, id: int) -> int:
        if READ_ONLY:
            self.loadPersistence()
            
        if self.__topics.get(topic_name) is None:
            raise TopicDoesNotExist("Topic -> {topic_name} does not exist")
        
        if not self.__authorizeConsumer(topic_name, id):
            raise UnAuthorizedAccess("Consumer {id} is not allowed to pull size from topic {topic_name}")
            
        total_size = 0
        partitions = self.__topics[topic_name].getPartitions()
        index = 0
        while index < len(partitions):            
            try:
                total_size += await partitions[index].getBroker().getSize(partitions[index].getId(), id)
            except BrokerDownError as e:
                await self.removeBroker(e.broker)
                partitions = self.__topics[topic_name].getPartitions()
                index = 0
                total_size = 0
            index += 1
        return total_size
    
    
        