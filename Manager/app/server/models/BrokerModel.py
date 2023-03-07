from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship

from ..database import Base

class BrokerModel(Base):
    __tablename__ = "broker"
    
    id = Column(Integer, primary_key=True, index=True)
    address = Column(String)

    partitions = relationship("PartitionModel", back_populates="broker")