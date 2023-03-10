import json
from decimal import Decimal
import time

import tronpy.exceptions
from flask import current_app, g
from tronpy import Tron
from tronpy.providers import HTTPProvider

from ..db import get_db, query_db
from ..utils import get_filter_config, get_tron_client, get_wallet_balance, estimateenergy
from ..logging import logger
from ..trc20wallet import Trc20Wallet
from ..wallet import Wallet
from ..block_scanner import BlockScanner
from . import api


@api.post("/generate-address")
def generate_new_address():

    client = Tron()
    addresses = client.generate_address()

    db = get_db()
    db.execute(
        "INSERT INTO keys (symbol, public, private, type) VALUES (?, ?, ?, 'onetime')",
        (g.symbol, addresses['base58check_address'], addresses['private_key']),
    )
    db.commit()

    BlockScanner.add_watched_account(addresses['base58check_address'])

    return {'status': 'success', 'base58check_address': addresses['base58check_address']}

@api.post('/balance')
def get_balance():
    start = time.time()

    w = Wallet(g.symbol)
    balance = w.balance
    return {'status': 'success', 'balance': balance, 'query_time': time.time() - start,}

@api.post('/status')
def get_status():
    bs = BlockScanner()
    last_seen_block_num = bs.get_last_seen_block_num()
    block =  bs.tron_client.get_block(last_seen_block_num)
    return {'status': 'success', 'last_block_timestamp': block['block_header']['raw_data']['timestamp'] // 1000}

@api.post('/transaction/<txid>')
def get_transaction(txid):
    tron_client = get_tron_client()
    tx = tron_client.get_transaction(txid)
    info = BlockScanner.get_tx_info(tx)
    try:
        latest_block_number = tron_client.get_latest_block_number()
        tx_block_number = tron_client.get_transaction_info(txid)['blockNumber']
        confirmations = latest_block_number - tx_block_number or 1
    except tronpy.exceptions.TransactionNotFound:
        logger.warning(f"Can't get confirmations for {txid}")
        confirmations = 1
    return {'address': info.to_addr, 'amount': info.amount, 'confirmations': confirmations, 'category': 'receive'}

@api.post('/dump')
def dump():
    rows = query_db('select * from keys where symbol = ? or type = "fee_deposit"', (g.symbol, ))
    keys = [{key: row[key] for key in ('public', 'private', 'type', 'symbol')} for row in rows]
    return {'accounts': keys}

@api.post('/fee-deposit-account')
def get_fee_deposit_account():
    client = get_tron_client()
    key = query_db('select * from keys where type = "fee_deposit"', one=True)
    try:
        balance = client.get_account_balance(key['public'])
    except tronpy.exceptions.AddressNotFound:
        balance = Decimal(0)
    return {'account': key['public'], 'balance': balance}

@api.post('/estimate-energy/<src>/<dst>/<decimal:amount>')
def estimate_energy(src, dst, amount):
    res = estimateenergy(src, dst, amount, g.symbol)
    logger.warning(f"estimateenergy result: {res}")
    return res