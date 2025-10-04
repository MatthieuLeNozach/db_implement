from sqlalchemy import Column, Integer, ForeignKey
from .base import Base

class MercurialeProductAssociation(Base):
    __tablename__ = "mercuriale_products"
    __table_args__ = {"extend_existing": True}

    id = Column(Integer, primary_key=True)
    mercuriale_id = Column(ForeignKey("mercuriale.id"))
    product_id = Column(ForeignKey("product.id"))
