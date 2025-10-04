import logging
from sqlalchemy import (
    Column, Integer, String, ForeignKey, DateTime, Float, Boolean
)
from sqlalchemy.orm import relationship, declarative_base
from sqlalchemy.exc import SQLAlchemyError

logger = logging.getLogger(__name__)
Base = declarative_base()


# -------------------------
# Association table with reduxcoef
# -------------------------

class MercurialeProductAssociation(Base):
    __tablename__ = "mercuriale_products"
    __table_args__ = {"extend_existing": True}  # <<< add this

    id = Column(Integer, primary_key=True)
    mercuriale_id = Column(ForeignKey("mercuriale.id"))
    product_id = Column(ForeignKey("product.id"))

# -------------------------
# Core tables
# -------------------------
class Mercuriale(Base):
    __tablename__ = "mercuriale"
    id = Column(Integer, primary_key=True)
    name = Column(String, nullable=False, unique=True)
    description = Column(String)

    # Relationships
    products = relationship(
        "Product",
        secondary="mercuriale_products",
        back_populates="mercuriales"
    )
    customers = relationship("Customer", back_populates="mercuriale")


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
        """Products accessible via assigned mercuriale."""
        return self.mercuriale.products if self.mercuriale else []


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

    # Reverse relationship
    mercuriales = relationship(
        "Mercuriale",
        secondary="mercuriale_products",
        back_populates="products"
    )


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


# -------------------------
# Customer assignment logic
# -------------------------
class CustomerAssignmentImporter:
    """
    Dynamically assigns customers to mercuriales based on rules
    using customer fields such as name2 and required_range.
    """

    ASSIGNMENT_RULES = [
        {
            "condition": lambda c: c.name2 and "COMPASS" in c.name2.upper(),
            "mercuriale_name": "compass",
        },
        {
            "condition": lambda c: c.name2 and any(x in c.name2.upper() for x in ["SODEXO", "SOGERES"]) and getattr(c, "required_range", False),
            "mercuriale_name": "sogeres",
        },
        {
            "condition": lambda c: c.name2 and any(x in c.name2.upper() for x in ["SODEXO", "SOGERES"]) and not getattr(c, "required_range", False),
            "mercuriale_name": "sodexo_open",
        },
        {
            "condition": lambda c: c.name2 and "ELIOR" in c.name2.upper(),
            "mercuriale_name": "elior_open",
        },
    ]

    def __init__(self, session):
        self.session = session

    def assign_mercuriale(self):
        customers = self.session.query(Customer).all()
        assigned_count = 0
        skipped_count = 0

        for customer in customers:
            assigned = False
            for rule in self.ASSIGNMENT_RULES:
                try:
                    if rule["condition"](customer):
                        mercuriale = self.session.query(Mercuriale).filter_by(
                            name=rule["mercuriale_name"]
                        ).first()
                        if mercuriale:
                            customer.mercuriale = mercuriale
                            assigned = True
                            assigned_count += 1
                            break
                        else:
                            logger.warning(f"Mercuriale {rule['mercuriale_name']} not found")
                except Exception as e:
                    logger.error(f"Error evaluating rule for customer {customer.customer_number}: {e}")

            if not assigned:
                skipped_count += 1
                logger.info(f"No rule matched for customer {customer.customer_number}")

        try:
            self.session.commit()
            logger.info(
                f"✅ Customer assignment complete: {assigned_count} assigned, {skipped_count} skipped"
            )
        except SQLAlchemyError as e:
            self.session.rollback()
            logger.error(f"❌ Commit failed: {e}")
            raise