from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from ..database import Base

class PartitionModel(Base):
    __tablename__ = "partition"
    
    id = Column(Integer, primary_key=True, index=True)
    
    broker_id = Column(Integer, ForeignKey("broker.id"))
    broker = relationship("BrokerModel", back_populates="partitions")
    
    topic_id = Column(Integer, ForeignKey("topic.id"))
    topic = relationship("TopicModel", back_populates="partitions")