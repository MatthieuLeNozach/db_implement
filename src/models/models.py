from datetime import datetime
from sqlalchemy import (
    Column, Integer, String, Boolean, ForeignKey, Float, DateTime
)
from sqlalchemy.orm import relationship, declarative_base

Base = declarative_base()

class CustomerAssignmentCondition(Base):
    __tablename__ = "customer_assignment_conditions"

    id = Column(Integer, primary_key=True)
    field = Column(String, nullable=False)
    operator = Column(String, nullable=False)
    value = Column(String, nullable=False)
    mercuriale_name = Column(String)
    priority = Column(Integer)
    required = Column(Boolean, default=False)
    

class MercurialeProductAssociation(Base):
    __tablename__ = "mercuriale_products"

    mercuriale_id = Column(Integer, ForeignKey("mercuriale.id"), primary_key=True)
    product_id = Column(Integer, ForeignKey("product.id"), primary_key=True)
    reduxcoef = Column(Float, default=1.0)


class CustomerAssignmentRule(Base):
    __tablename__ = "customer_assignment_rule"

    id = Column(Integer, primary_key=True)
    field = Column(String, nullable=False)            # column name to check
    operator = Column(String, nullable=False)         # equals / contains
    value = Column(String, nullable=False)            # value to compare
    mercuriale_name = Column(String, nullable=False)  # target mercuriale
    priority = Column(Integer, default=99)            # evaluation order
    required = Column(Boolean, default=True)          # must match


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


class FormatConfig(Base):
    __tablename__ = "format_config"

    id = Column(Integer, primary_key=True)
    format_name = Column(String, unique=True, nullable=False)
    po_number_fuzzy = Column(String, nullable=True)
    delivery_date_regex = Column(String, nullable=True)        # semicolon-separated if multiple
    entity_code_regex = Column(String, nullable=True)          # semicolon-separated if multiple
    entity_name_regex = Column(String, nullable=True)
    header_fuzzy = Column(String, nullable=False)
    skip_footer_keywords = Column(String, nullable=True)       # semicolon-separated
    min_columns = Column(Integer, default=3)
    fuzzy_threshold = Column(Float, default=0.8)
    column_description = Column(String, nullable=False)        # semicolon-separated possible names
    column_sku = Column(String, nullable=False)
    column_quantity = Column(String, nullable=False)
    column_unit = Column(String, nullable=True)
    customer_matching_strategies = Column(String, nullable=True) # semicolon-separated
    company_name_patterns = Column(String, nullable=True)       # semicolon-separated


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


class PurchaseOrder(Base):
    __tablename__ = "purchase_order"

    id = Column(Integer, primary_key=True)
    customer_id = Column(Integer, ForeignKey("customer.id"))
    po_number = Column(String, nullable=False, unique=True)
    delivery_date = Column(DateTime, nullable=True)
    entity_code = Column(String, nullable=True)
    entity_name = Column(String, nullable=True)
    customer_number = Column(String, nullable=True)
    file_name = Column(String, nullable=True)
    processing_date = Column(DateTime, default=datetime.utcnow)
    processing_duration = Column(Float, nullable=True)  # 👈 NEW FIELD (seconds)
    date = Column(DateTime, default=datetime.utcnow)

    customer = relationship("Customer", back_populates="orders")
    lines = relationship("PurchaseOrderLine", back_populates="order", cascade="all, delete-orphan")



class PurchaseOrderLine(Base):
    __tablename__ = "purchase_order_line"

    id = Column(Integer, primary_key=True)
    order_id = Column(Integer, ForeignKey("purchase_order.id"))
    product_id = Column(Integer, ForeignKey("product.id"), nullable=True)
    sku = Column(String, nullable=True)
    description = Column(String, nullable=True)
    quantity = Column(Integer)
    unit = Column(String, nullable=True)
    comment = Column(String, nullable=True)

    # Relationships
    order = relationship("PurchaseOrder", back_populates="lines")
    product = relationship("Product")

