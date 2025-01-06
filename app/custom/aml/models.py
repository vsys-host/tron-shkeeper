from datetime import datetime
from decimal import Decimal

from sqlmodel import Field, SQLModel, Column
from sqlalchemy import DateTime, func

from ...schemas import TronSymbol, TronAddress


class Transaction(SQLModel, table=True):
    __tablename__ = "tron_aml_transactions"

    id: int | None = Field(default=None, primary_key=True)
    tx_id: str
    status: str
    ttype: str
    score: Decimal = Field(default=-1, max_digits=7, decimal_places=5)
    crypto: TronSymbol
    amount: Decimal = Field(default=0, max_digits=52, decimal_places=18)
    address: TronAddress
    uid: str | None = None
    data: str | None = None
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )


class Payout(SQLModel, table=True):
    __tablename__ = "tron_aml_payouts"

    id: int | None = Field(default=None, primary_key=True)
    tx_id: str
    external_tx_id: str
    status: str | None = None
    dtype: str | None = None
    crypto: TronSymbol
    amount_calc: Decimal = Field(default=0, max_digits=52, decimal_places=18)
    amount_send: Decimal = Field(default=0, max_digits=52, decimal_places=18)
    address: TronAddress
    created_at: datetime = Field(sa_column=Column(DateTime, default=func.now()))
    updated_at: datetime = Field(
        sa_column=Column(DateTime, default=func.now(), onupdate=func.now())
    )
