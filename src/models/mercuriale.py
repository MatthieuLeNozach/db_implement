from sqlalchemy import Column, Integer, String, Float, ForeignKey
from sqlalchemy.orm import relationship
from .base import Base

class MercurialeProductAssociation(Base):
    __tablename__ = "mercuriale_products"

    mercuriale_id = Column(Integer, ForeignKey("mercuriale.id"), primary_key=True)
    product_id = Column(Integer, ForeignKey("product.id"), primary_key=True)
    reduxcoef = Column(Float, default=1.0)


class Mercuriale(Base):
    __tablename__ = "mercuriale"

    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String)

    products = relationship(
        "Product",
        secondary="mercuriale_products",
        back_populates="mercuriales"
    )
    customers = relationship("Customer", back_populates="mercuriale")
