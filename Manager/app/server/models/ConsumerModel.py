from sqlalchemy import Boolean, Column, ForeignKey, Integer, String, Table
from sqlalchemy.orm import relationship

from ..database import Base

class ConsumerModel(Base):
    __tablename__ = "consumer"
    
    id = Column(Integer, primary_key=True, index=True)

    topic_id = Column(Integer, ForeignKey("topic.id"))
    topic = relationship("TopicModel", back_populates="consumers")