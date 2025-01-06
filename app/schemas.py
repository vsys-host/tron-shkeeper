from decimal import Decimal
from enum import Enum
from functools import cache
from typing import Annotated, Any, List, Literal

from pydantic import (
    AfterValidator,
    BaseModel,
    Json,
)
import tronpy


class KeyType(str, Enum):
    fee_deposit = "fee_deposit"
    onetime = "onetime"


class TronNetwork(str, Enum):
    mainnet = "main"
    testnet = "nile"


class TronFullnode(BaseModel):
    name: str
    url: str


class TronSymbol(str, Enum):
    TRX = "TRX"
    USDT = "USDT"
    USDC = "USDC"


def is_tron_address(value: str) -> str:
    if tronpy.keys.is_base58check_address(value):
        return value
    raise ValueError(
        f"{value} is not a Tron address or it is not in base58 check format"
    )


TronAddress = Annotated[
    str,
    AfterValidator(is_tron_address),
]


class TronTransaction(BaseModel):
    status: str
    txid: str
    symbol: TronSymbol
    src_addr: TronAddress
    dst_addr: TronAddress
    amount: Decimal
    is_trc20: bool


class Token(BaseModel):
    symbol: TronSymbol
    contract_address: TronAddress
    min_transfer_threshold: Decimal
    network: TronNetwork
    decimal: int
