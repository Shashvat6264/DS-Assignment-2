from ..models import MessageModel

class Message:
    id = 0
    def __init__(self, message) -> None:
        self.__message = message
        self.__id = Message.id
        Message.id += 1
        
    async def save(self, database = None):
        if database is not None:
            await database.create(MessageModel, id=self.__id, message=self.__message)
    
    def getMessage(self):
        return self.__message
    
    def getId(self):
        return self.__id
    
    def __setId(self, id):
        self.__id = id
    
    def modelToObj(instance):
        message = Message(message=instance.message)
        message.__setId(instance.id)
        return message