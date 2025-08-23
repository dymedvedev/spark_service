from pydantic import BaseModel, Field
from typing import List, Optional
from datetime import datetime


class ReceiptItemCreate(BaseModel):
    name: str = Field(..., description="Название товара/услуги")
    price: float = Field(..., description="Цена за единицу")
    quantity: int = Field(..., description="Количество")
    sum: float = Field(..., description="Общая сумма")
    invoice_type: Optional[int] = Field(None, description="Тип счета")
    invoice_sum: Optional[float] = Field(None, description="Сумма счета")
    product_type: int = Field(..., description="Тип продукта")
    payment_type: int = Field(..., description="Тип оплаты")
    organization_form: str = Field(..., description="Форма организации")


class ReceiptCreate(BaseModel):
    items: List[ReceiptItemCreate] = Field(..., description="Список товаров/услуг")
    id: int = Field(..., description="ID чека")
    user: int = Field(..., description="ID пользователя")
    created_at: datetime = Field(..., description="Дата создания")
    total_sum: Optional[float] = Field(None, description="Общая сумма чека")


class ReceiptResponse(BaseModel):
    id: int
    user: int
    created_at: datetime
    total_sum: float
    items_count: int
    
    class Config:
        from_attributes = True


class AnalyticsResponse(BaseModel):
    monthly_receipts: int
    payment_types: dict
    avg_receipt: float
    total_receipts: int
    revenue: float
