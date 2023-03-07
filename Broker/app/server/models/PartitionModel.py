from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from ..database import Base

class PartitionModel(Base):
    __tablename__ = "partition"
    
    id = Column(Integer, primary_key=True, index=True)
    topicname = Column(Integer, index=True)
    messages = relationship("MessageModel", back_populates="partition")
    consumers = relationship("ConsumerModel", back_populates="partition")
    