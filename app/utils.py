from dataclasses import dataclass
from functools import wraps
import logging
from decimal import Decimal
import math
import time
from typing import Literal
import concurrent

import tronpy.exceptions
from flask import current_app
from tronpy import Tron
from tronpy.keys import PrivateKey
from tronpy.abi import trx_abi
from werkzeug.routing import BaseConverter
import requests

from .config import config
from .db import get_db, query_db, query_db2
from .logging import logger
from .connection_manager import ConnectionManager
from .wallet_encryption import wallet_encryption


class DecimalConverter(BaseConverter):
    def to_python(self, value):
        return Decimal(value)

    def to_url(self, value):
        return BaseConverter.to_url(value)


def get_filter_config():
    with current_app.app_context():
        return {
            row["public"]: row["symbol"]
            for row in query_db(
                'select public, symbol from keys where type = "onetime"'
            )
        }


def init_wallet(app):
    with app.app_context():
        main_key = query_db('select * from keys where type = "fee_deposit"', one=True)
        if main_key:
            logger.info("Fee deposit account is already exists.")
        else:
            addresses = Tron().generate_address()
            db = get_db()
            db.execute(
                "INSERT INTO keys (symbol, public, private, type) VALUES ('_', ?, ?, 'fee_deposit')",
                (
                    addresses["base58check_address"],
                    wallet_encryption.encrypt(addresses["private_key"]),
                ),
            )
            db.commit()
            logger.info("Fee deposit account has been created.")


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


def has_free_bw(account, tx_bw):
    acc_res = ConnectionManager.client().get_account_resource(account)
    daily_bw = acc_res.get("freeNetLimit", 0) - acc_res.get("freeNetUsed", 0)
    staked_bw = acc_res.get("NetLimit", 0) - acc_res.get("NetUsed", 0)
    logger.info(f"Account {account} has {staked_bw=} {daily_bw=}")
    if staked_bw < tx_bw:
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
