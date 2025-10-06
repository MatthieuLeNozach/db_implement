# src/models/assignment_rules.py
from sqlalchemy import Column, Integer, String, Boolean
from .base import Base

class CustomerAssignmentRule(Base):
    __tablename__ = "customer_assignment_rule"

    id = Column(Integer, primary_key=True)
    field = Column(String, nullable=False)            # column name to check
    operator = Column(String, nullable=False)         # equals / contains
    value = Column(String, nullable=False)            # value to compare
    mercuriale_name = Column(String, nullable=False)  # target mercuriale
    priority = Column(Integer, default=99)            # evaluation order
    required = Column(Boolean, default=True)          # must match
