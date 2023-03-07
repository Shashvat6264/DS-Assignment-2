import requests
from .client import Client
from .exceptions import *
from .utils import *

class Producer(Client):
    def __init__(self, topics, broker):
        super().__init__()
        self.broker = broker
        self.register(topics)
    
    @convertToList
    def register(self, topics):
        for topic in topics:
            if topic in self.topicIds.keys():
                raise AlreadyRegistered("Already registered for topic: " + topic)
            registerResponse = requests.post(self.broker + '/producer/register', json = {'topic_name': str(topic)})
            if registerResponse.status_code == 200:
                self.topicIds[topic] = registerResponse.json()['producer_id']
                self.threadIds[topic] = HeartBeatThread(self.manager, self.topicIds[topic])
                self.threadIds[topic].start()
            else:
                raise Exception(registerResponse.json()["message"])
    
    @convertToList
    def enqueue(self, topics, message, partition_key=None):
        for topic in topics:
            if topic not in self.topicIds.keys():
                raise UnauthorizedException("This producer is not authorized to push messages to topic: " + topic)
            enqueueResponse = requests.post(self.broker + '/producer/produce', json = {'topic_name': topic, 'producer_id': self.topicIds[topic], 'message': message, 'partition_key': partition_key})
            if enqueueResponse.status_code == 200:
                continue
            elif enqueueResponse.status_code == 401:
                raise UnauthorizedException(enqueueResponse.json()["message"])
            else:
                raise TopicDoesNotExist(enqueueResponse.json()["message"])
        