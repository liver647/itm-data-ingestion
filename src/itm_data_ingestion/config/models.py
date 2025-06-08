"""A module containing the Pydantic models for the data ingestion."""


from pydantic import BaseModel, Field, field_validator

from typing import Optional

from datetime import datetime

class Client(BaseModel):
    """
    """

    id: int
    name: str = Field(min_length=1)
    job: str = Field(min_length=1)
    email: str = Field(min_length=1)
    account_id: Optional[int]

class GpsCoordinate(BaseModel):
    """"""
    latitude: float
    longitude: float

class Store(BaseModel):
    """
    """

    id: int
    latlng: GpsCoordinate
    opening: str = Field(min_length=1)
    closing: str = Field(min_length=1)
    type: str = Field(min_length=1)


class Product(BaseModel):
    """"""

    id: int
    ean: str = Field(min_length=1)
    brand: str = Field(min_length=1)
    description: Optional[str]

class Transaction(BaseModel):
    """"""

    transaction_id: int
    client_id: int
    date: str
    hour: int
    minute: int
    product_id: int
    quantity: int
    store_id: int
    account_id: Optional[str] = None
    ts: datetime

