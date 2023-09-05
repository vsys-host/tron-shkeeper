from dataclasses import dataclass
from functools import wraps
import logging
from decimal import Decimal
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

from .config import config, get_contract_address
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
        return { row['public']: row['symbol']
                 for row in query_db('select public, symbol from keys where type = "onetime"') }

def init_wallet(app):
    with app.app_context():
        main_key = query_db('select * from keys where type = "fee_deposit"', one=True)
        if main_key:
            logger.info('Fee deposit account is already exists.')
        else:
            addresses = Tron().generate_address()
            db = get_db()
            db.execute(
                "INSERT INTO keys (symbol, public, private, type) VALUES ('_', ?, ?, 'fee_deposit')",
                (addresses['base58check_address'], wallet_encryption.encrypt(addresses['private_key'])),
            )
            db.commit()
            logger.info('Fee deposit account has been created.')

def estimateenergy(src, dst, amount, symbol):
    tron_client = ConnectionManager.client()

    parameter = trx_abi.encode_single("(address,uint256)", [dst, int(amount * 1_000_000)]).hex()
    data = {
        "owner_address": src,
        "contract_address": get_contract_address(symbol),
        "function_selector": "transfer(address,uint256)",
        "parameter": parameter,
        "visible": True
    }
    return tron_client.provider.make_request('/wallet/estimateenergy', params=data)

def skip_if_running(f):
    task_name = f'{f.__module__}.{f.__name__}'

    @wraps(f)
    def wrapped(self, *args, **kwargs):
        workers = self.app.control.inspect().active()

        for worker, tasks in workers.items():
            for task in tasks:
                if (task_name == task['name'] and
                        tuple(args) == tuple(task['args']) and
                        kwargs == task['kwargs'] and
                        self.request.id != task['id']):
                    logger.debug(f'task {task_name} ({args}, {kwargs}) is running on {worker}, skipping')

                    return 'skipped (already running)'
        logger.debug(f'task {task_name} ({args}, {kwargs}) is allowed to run')
        return f(self, *args, **kwargs)

    return wrapped