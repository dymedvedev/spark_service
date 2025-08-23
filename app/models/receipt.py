from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Text
from sqlalchemy.orm import relationship
from sqlalchemy.sql import func
from .database import Base


class User(Base):
    __tablename__ = "users"
    
    user_id = Column(Integer, primary_key=True, index=True)


class Organization(Base):
    __tablename__ = "organizations"
    
    org_id = Column(Integer, primary_key=True, index=True)
    org_name = Column(String(255), nullable=False)
    legal_form = Column(String(100), nullable=False)


class Invoice(Base):
    __tablename__ = "invoices"
    
    invoice_id = Column(Integer, primary_key=True, index=True)
    invoice_sum = Column(Float, nullable=True)
    invoice_name = Column(String(255), nullable=True)
    payment_type = Column(Integer, nullable=True)


class Check(Base):
    __tablename__ = "checks"
    
    check_id = Column(Integer, primary_key=True, index=True)
    created_at = Column(DateTime(timezone=True), server_default=func.now())
    check_sum = Column(Float, nullable=False)
    org_id = Column(Integer, ForeignKey("organizations.org_id"), nullable=True)
    user_id = Column(Integer, ForeignKey("users.user_id"), nullable=True)
    invoice_id = Column(Integer, ForeignKey("invoices.invoice_id"), nullable=True)
    
    items = relationship("Item", back_populates="check")
    user = relationship("User", foreign_keys=[user_id])
    organization = relationship("Organization", foreign_keys=[org_id])
    invoice = relationship("Invoice", foreign_keys=[invoice_id])


class Item(Base):
    __tablename__ = "items"
    
    item_id = Column(Integer, primary_key=True, index=True)
    item_name = Column(String(255), nullable=False)
    item_price = Column(Float, nullable=False)
    item_type = Column(Integer, nullable=False)
    item_quantity = Column(Integer, nullable=False)
    item_sum = Column(Float, nullable=False)
    check_id = Column(Integer, ForeignKey("checks.check_id"))
    
    check = relationship("Check", back_populates="items")
