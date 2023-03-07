class WriteInReadManager(Exception):
    "When trying to write in a read-only manager"
    def __init__(self, message: str) -> None:
        super().__init__(message)
    
class NoBrokerPresent(Exception):
    "Raised when manager has no broker"
    def __init__(self, message: str) -> None:
        super().__init__(message)
        
class BrokerAlreadyPresent(Exception):
    "Raised when a broker is already present"
    def __init__(self, message: str) -> None:
        super().__init__(message)
    
class TopicDoesNotExist(Exception):
    "Raised when topic does not exist"
    def __init__(self, message: str) -> None:
        super().__init__(message)
        
class TopicAlreadyExists(Exception):
    "Raised when topic already exists"
    def __init__(self, message: str) -> None:
        super().__init__(message)
        
class PartitionNotInTopic(Exception):
    "Raised when referenced a partition in a topic which does not exist"
    def __init__(self, message: str) -> None:
        super().__init__(message)
        
class BrokerError(Exception):
    "When an error is returned from broker's side"
    def __init__(self, broker: str, message: str) -> None:
        super().__init__("Broker -> {broker} " + message)

class BrokerDownError(Exception):
    "When broker is not to be found"
    def __init__(self, broker, message: str) -> None:
        self.broker = broker
        address = broker.getAddress()
        super().__init__("Broker -> " + address + " " + message)
        
class UnAuthorizedAccess(Exception):
    "When unauthorized client tries to push or pull"
    def __init__(self, message: str) -> None:
        super().__init__(message)

class QueueEmpty(Exception):
    "When the queue is empty"
    def __init__(self, message: str) -> None:
        super().__init__(message)