from sqlalchemy import Column, Integer, String
from sqlalchemy.orm import relationship
from .base import Base

class Product(Base):
    __tablename__ = "product"

    id = Column(Integer, primary_key=True)
    sku = Column(String, unique=True, nullable=False)
    description = Column(String)
    supplier_number = Column(String)
    product_family = Column(String)
    sub_family = Column(String)
    sub_sub_family = Column(String)
    sub_sub_sub_family = Column(String)
    brand = Column(String)

    mercuriales = relationship(
        "Mercuriale",
        secondary="mercuriale_products",
        back_populates="products"
    )
