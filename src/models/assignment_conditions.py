from sqlalchemy import Column, Integer, String, Boolean
from .base import Base

class CustomerAssignmentCondition(Base):
    __tablename__ = "customer_assignment_conditions"

    id = Column(Integer, primary_key=True)
    field = Column(String, nullable=False)
    operator = Column(String, nullable=False)
    value = Column(String, nullable=False)
    mercuriale_name = Column(String)
    priority = Column(Integer)
    required = Column(Boolean, default=False)
