from sqlalchemy import Column, Integer, String, Boolean, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class Customer(Base):
    __tablename__ = "customer"

    id = Column(Integer, primary_key=True)
    customer_number = Column(String, unique=True, nullable=False)
    name = Column(String, nullable=False)
    name2 = Column(String)
    delivery_zone = Column(String)
    postal_code = Column(String)
    city = Column(String)
    required_range = Column(Boolean, default=False)
    client_type = Column(String)
    sub_client_type = Column(String)

    mercuriale_id = Column(Integer, ForeignKey("mercuriale.id"))
    mercuriale = relationship("Mercuriale", back_populates="customers")

    orders = relationship("PurchaseOrder", back_populates="customer")

    @property
    def allowed_products(self):
        return self.mercuriale.products if self.mercuriale else []
