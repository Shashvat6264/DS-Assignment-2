from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from ..database import Base

class TopicModel(Base):
    __tablename__ = "topic"
    
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True)
    
    partitions = relationship("PartitionModel", back_populates="topic")
    consumers = relationship("ConsumerModel", back_populates="topic")
    producers = relationship("ProducerModel", back_populates="topic")