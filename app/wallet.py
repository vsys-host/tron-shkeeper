import collections
import concurrent
from copy import copy
from functools import lru_cache
import sqlite3
import time
from dataclasses import dataclass
from decimal import Decimal
from typing import List, Literal

import tronpy.exceptions
from tronpy.keys import PrivateKey

from .config import config, get_contract_address
from .db import query_db2
from .logging import logger
from .connection_manager import ConnectionManager


class Wallet:

    CACHE = {
        'decimals': {},
        'contracts': {},
    }
    main_account = query_db2('select * from keys where type = "fee_deposit" ', one=True)


    def __init__(self, symbol='TRX'):
        self.symbol = symbol
        self.client = ConnectionManager.client()
        if symbol != 'TRX':
            self.contract_address = get_contract_address(symbol)

    def get_contract(self, contract_address=None):
        if contract_address is None:
            contract_address = self.contract_address
        contract = self.CACHE['contracts'].get(contract_address)
        if not contract:
            contract = self.client.get_contract(contract_address)
            self.CACHE['contracts'][contract_address] = contract
        decimals = self.CACHE['decimals'].get(contract_address)
        if not decimals:
            self.CACHE['decimals'][contract_address] = contract.functions.decimals()
        return contract

    @property
    def balance(self):
        if self.symbol == 'TRX':
            try:
                return self.client.get_account_balance(self.main_account['public'])
            except tronpy.exceptions.AddressNotFound:
                return Decimal(0)
        else:
            return Decimal(self.get_contract().functions.balanceOf(self.main_account['public'])) \
                   / 10 ** self.CACHE['decimals'][self.contract_address]

    def transfer(self, dst, amount):

        if self.symbol == 'TRX':
            txn = self.client.trx.transfer(self.main_account['public'], dst, int(amount * 1_000_000))
        else:
            txn = (self.get_contract().functions.transfer(dst, int(amount * 1_000_000))
                                                .with_owner(self.main_account['public'])
                                                .fee_limit(int(config['TX_FEE_LIMIT'] * 1_000_000)))

        txn._raw_data['expiration'] += 60_000
        txn = (txn.build()
                  .sign(PrivateKey(bytes.fromhex(self.main_account['private']))))
        txn_res = (txn.broadcast()
                      .wait())

        logger.info(f"{amount} {self.symbol} has been sent to {dst} with TXID {txn.txid}. Details: {txn_res}")

        result = {
            "dest": dst,
            "amount": str(amount),
            "txids": [txn.txid],
            "details": txn_res,
        }

        if self.symbol == 'TRX':
            if txn_res['contractResult'] == ['']:
                result['status'] = 'success'
            else:
                result['status'] = 'error'
                result['message'] = f"contractResult: {txn_res['contractResult']}"
        else:
            if txn_res['receipt']['result'] == 'SUCCESS':
                result['status'] = 'success'
            else:
                result['status'] = 'error'
                result['message'] = f"{txn_res['result']}: {txn_res['resMessage']}"

        return result