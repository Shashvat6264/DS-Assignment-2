from sqlalchemy import Boolean, Column, ForeignKey, Integer, String
from sqlalchemy.orm import relationship

from ..database import Base


class ConsumerModel(Base):
    __tablename__ = "consumer"
    id = Column(Integer, primary_key=True, index=True)
    pid = Column(Integer, index=True)
    offset = Column(Integer)
    partition_id = Column(Integer, ForeignKey("partition.id"))
    
    partition = relationship("PartitionModel", back_populates="consumers")
    