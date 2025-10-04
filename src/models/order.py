from sqlalchemy import Column, Integer, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from .base import Base

class PurchaseOrder(Base):
    __tablename__ = "purchase_order"

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customer.id"))
    date = Column(DateTime)

    customer = relationship("Customer", back_populates="orders")
    lines = relationship("PurchaseOrderLine", back_populates="order")


class PurchaseOrderLine(Base):
    __tablename__ = "purchase_order_line"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("purchase_order.id"))
    product_id = Column(Integer, ForeignKey("product.id"))
    quantity = Column(Integer)

    order = relationship("PurchaseOrder", back_populates="lines")
    product = relationship("Product")
