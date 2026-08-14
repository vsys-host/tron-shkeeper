from dataclasses import dataclass
from functools import wraps
import logging
from decimal import Decimal
import math
import time
from typing import Literal
import concurrent

import tronpy.exceptions
from flask import Flask, current_app
from tronpy import Tron
from tronpy.keys import PrivateKey
from tronpy.abi import trx_abi
from werkzeug.routing import BaseConverter
import requests

from app.schemas import KeyType, TronAddress

from .config import config
from .logging import logger
from .connection_manager import ConnectionManager
from .wallet_encryption import wallet_encryption
from .repositories import KeyRepository


key_repository = KeyRepository(store_id=1)


def _tenant_logger(store_id: int):
    class TenantLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            store_id = self.extra.get("store_id") if self.extra else None
            return f"[store_id={store_id}] {msg}", kwargs

    return TenantLoggerAdapter(logger, {"store_id": store_id})


class DecimalConverter(BaseConverter):
    def to_python(self, value):
        return Decimal(value)

    def to_url(self, value):
        return BaseConverter.to_url(value)




def add_key(type: KeyType, public=None, uniq_type=True, store_id: int = 1):
    logger = _tenant_logger(store_id)
    key_repository = KeyRepository(store_id=store_id)
    key = key_repository.get_by_type(type)
    if key and uniq_type:
        logger.info(f"{type} account is already exists.")
    else:
        addresses = Tron().generate_address()
        key_repository.create_external_key(
            type,
            public if public else addresses["base58check_address"],
            "EXTERNALLY_MANAGED"
            if public
            else wallet_encryption.encrypt(addresses["private_key"]),
        )
        logger.info(f"{type} account has been created.")


def get_key(
    type: KeyType, pub: str | None = None, store_id: int = 1
) -> tuple[PrivateKey | None, str]:
    logger = _tenant_logger(store_id)
    key_repository = KeyRepository(store_id=store_id)
    key = (
        key_repository.get_by_public_and_type(pub, type)
        if pub
        else key_repository.get_by_type(type)
    )
    if not key:
        logger.error(f"No key found for type {type}")
        return None, ""
    if key["private"] == "EXTERNALLY_MANAGED":
        return None, key["public"]
    private_key = wallet_encryption.decrypt(key["private"])
    return PrivateKey(bytes.fromhex(private_key)), key["public"]


def init_wallet(app: Flask, store_id: int = 1):
    with app.app_context():
        add_key(KeyType.fee_deposit, store_id=store_id)
        if (
            config.ENERGY_DELEGATION_MODE
            and config.ENERGY_DELEGATION_MODE_SEPARATE_BALANCE_AND_ENERGY_ACCOUNTS
        ):
            if config.ENERGY_DELEGATION_MODE_ENERGY_ACCOUNT_PUB_KEY:
                add_key(
                    KeyType.energy,
                    public=config.ENERGY_DELEGATION_MODE_ENERGY_ACCOUNT_PUB_KEY,
                    store_id=store_id,
                )
            else:
                add_key(
                    KeyType.energy,
                    store_id=store_id,
                )


def get_energy_delegator(store_id: int = 1) -> tuple[PrivateKey, str]:
    if (
        config.ENERGY_DELEGATION_MODE
        and config.ENERGY_DELEGATION_MODE_SEPARATE_BALANCE_AND_ENERGY_ACCOUNTS
    ):
        if config.ENERGY_DELEGATION_MODE_ENERGY_ACCOUNT_PUB_KEY:
            # If an energy account public key is provided, assume the fee_deposit account
            # has permission to delegate energy from the energy account
            priv, _ = get_key(KeyType.fee_deposit, store_id=store_id)
            _, pub = get_key(KeyType.energy, store_id=store_id)
            return priv, pub
        else:
            return get_key(KeyType.energy, store_id=store_id)
    else:
        return get_key(KeyType.fee_deposit, store_id=store_id)


def estimateenergy(src, dst, amount, symbol):
    tron_client = ConnectionManager.client()

    parameter = trx_abi.encode_single(
        "(address,uint256)", [dst, int(amount * 1_000_000)]
    ).hex()
    data = {
        "owner_address": src,
        "contract_address": config.get_contract_address(symbol),
        "function_selector": "transfer(address,uint256)",
        "parameter": parameter,
        "visible": True,
    }
    return tron_client.provider.make_request("/wallet/estimateenergy", params=data)


def skip_if_running(f):
    task_name = f"{f.__module__}.{f.__name__}"

    @wraps(f)
    def wrapped(self, *args, **kwargs):
        workers = self.app.control.inspect().active()

        if workers:
            for worker, tasks in workers.items():
                for task in tasks:
                    if (
                        task_name == task["name"]
                        and tuple(args) == tuple(task["args"])
                        and kwargs == task["kwargs"]
                        and self.request.id != task["id"]
                    ):
                        return f"task {task_name} ({args}, {kwargs}) is already running on {worker}, skipping"
        return f(self, *args, **kwargs)

    return wrapped


def short_txid(txid: str, len=4) -> str:
    return f"{txid[:len]}..{txid[-len:]}"


def has_free_bw(account, tx_bw, use_only_staked=False):
    acc_res = ConnectionManager.client().get_account_resource(account)
    daily_bw = acc_res.get("freeNetLimit", 0) - acc_res.get("freeNetUsed", 0)
    staked_bw = acc_res.get("NetLimit", 0) - acc_res.get("NetUsed", 0)
    logger.info(f"Account {account} has {staked_bw=} {daily_bw=}")
    if staked_bw < tx_bw:
        if use_only_staked:
            logger.info("use_only_staked=True, skipping free(daily) bandwidth check")
            return False
        if daily_bw < tx_bw:
            return False
        else:
            logger.info(f"Account {account} will use daily bandwith")
    else:
        logger.info(f"Account {account} will use bandwith obtained from staking")
    return True


def est_vote_tx_bw_cons(num_of_votes):
    return math.ceil(244 + (num_of_votes * 30))


def estimate_bw_by_raw_data_hex(raw_data_hex: str):
    # https://developers.tron.network/docs/faq#5-how-to-calculate-the-bandwidth-and-energy-consumed-when-callingdeploying-a-contract
    DATA_HEX_PROTOBUF_EXTRA = 3
    MAX_RESULT_SIZE_IN_TX = 64
    A_SIGNATURE = 67
    MARGIN = 10
    return int(
        len(raw_data_hex) / 2
        + DATA_HEX_PROTOBUF_EXTRA
        + MAX_RESULT_SIZE_IN_TX
        + A_SIGNATURE
        + MARGIN
    )
