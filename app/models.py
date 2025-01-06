from datetime import datetime
from decimal import Decimal
from typing import Literal

from sqlmodel import Field, SQLModel, Column
from sqlalchemy import DateTime, UniqueConstraint, func

from .schemas import KeyType, TronSymbol, TronAddress


class Setting(SQLModel, table=True):
    __tablename__ = "tron_settings"

    name: str = Field(primary_key=True)
    value: str
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class Key(SQLModel, table=True):
    __tablename__ = "tron_keys"

    id: int | None = Field(default=None, primary_key=True)
    symbol: TronSymbol
    type: KeyType
    public: TronAddress
    private: str
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class Balance(SQLModel, table=True):
    __tablename__ = "tron_balances"
    __table_args__ = (UniqueConstraint("account", "symbol"),)

    id: int | None = Field(default=None, primary_key=True)
    account: TronAddress
    symbol: TronSymbol
    balance: Decimal = Field(default=0, max_digits=52, decimal_places=18)
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )
