from datetime import datetime
from decimal import Decimal

from sqlmodel import Field, SQLModel, Column
from sqlalchemy import DateTime, UniqueConstraint, func

from .schemas import TronSymbol, TronAddress

# NOTE: `Setting` (tron_settings) and `Key` (tron_keys) ORM models used to live
# here, but they were never queried anywhere in the codebase - the real
# `settings`/`keys` tables are accessed via the raw query_db/query_db2 layer in
# app/db.py (see app/schema.sql). They were removed as dead code as part of the
# SQLite -> MySQL migration.


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
