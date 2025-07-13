from pydantic import BaseModel
from typing import Literal
from datetime import datetime

class MovieEvent(BaseModel):
    movie_id: int
    title: str
    action: Literal["viewed", "added", "updated"]
    user_id: int

    class Config:
        extra = "allow"

class UserEvent(BaseModel):
    user_id: int
    username: str
    action: Literal["logged_in", "registered", "updated"]
    timestamp: datetime

    class Config:
        extra = "allow"

class PaymentEvent(BaseModel):
    payment_id: int
    user_id: int
    amount: float
    status: Literal["completed", "failed", "pending"]
    timestamp: datetime
    method_type: str

    class Config:
        extra = "allow"
