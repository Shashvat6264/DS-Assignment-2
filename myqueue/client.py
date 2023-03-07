import requests
from .exceptions import *
from .utils import *

class Client:
    def __init__(self, manager):
        self.manager = manager
        self.topicIds = {}
        self.threadIds = {}
    
    @convertToList
    def create_topic(self, topics, number_of_partitions: int = 3):
        for topic in topics:
            registerResponse = requests.post(self.manager + '/topics', json = {'topic_name': topic, 'number_of_partitions': number_of_partitions})
            if registerResponse.status_code != 201:
                raise TopicAlreadyExists(registerResponse.json()["message"])
            
    def list_topics(self):
        response = requests.get(self.manager + '/topics')
        if response.status_code == 200:
            return response.json()["topics"]
        else:
            raise Exception(response.json()["message"])
        
    def list_brokers(self):
        response = requests.get(self.manager + '/brokers')
        if response.status_code == 200:
            return response.json()["brokers"]
        
    @convertToList
    def add_broker(self, brokers):
        for broker in brokers:
            response = requests.post(self.manager + '/brokers', json={'broker_address': broker})
            if response.status_code != 201:
                raise BrokerAlreadyPresent(f"Broker {broker} is already present")
    
    
    