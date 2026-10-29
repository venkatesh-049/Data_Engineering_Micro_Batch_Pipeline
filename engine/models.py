from datetime import datetime
from enum import Enum
from typing import Optional
from pydantic import BaseModel, ConfigDict, Field, field_validator


class OrderStatus(str, Enum):
    CREATED = "CREATED"
    PAID = "PAID"
    CANCELLED = "CANCELLED"
    REFUNDED = "REFUNDED"


class RawOrderEvent(BaseModel):
    model_config = ConfigDict(extra="forbid")

    event_id: str
    event_time: datetime
    customer_id: str
    order_id: str
    currency: str = Field(min_length=3, max_length=3)
    gross_amount: float
    order_status: OrderStatus
    client_version: Optional[str] = None

    @field_validator("gross_amount")
    @classmethod
    def validate_gross_amount(cls, v: float) -> float:
        if v < 0:
            raise ValueError(f"gross_amount cannot be negative: {v}")
        return round(v, 4)

    @field_validator("currency")
    @classmethod
    def validate_currency(cls, v: str) -> str:
        v_upper = v.upper()
        if v_upper not in {"USD", "EUR", "GBP", "JPY"}:
            raise ValueError(f"Unsupported currency: {v}")
        return v_upper

    @field_validator("event_id", "customer_id", "order_id")
    @classmethod
    def validate_non_empty_identifier(cls, v: str) -> str:
        cleaned = v.strip()
        if not cleaned:
            raise ValueError("identifier fields cannot be empty")
        return cleaned