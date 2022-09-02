import json
from decimal import Decimal

import tronpy.exceptions
from flask import current_app, g
from tronpy import Tron
from tronpy.providers import HTTPProvider

from .. import events
from ..config import get_contract_address
from ..db import get_db, query_db
from ..utils import get_confirmations, get_filter_config, get_tron_client
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

    events.FILTER = get_filter_config()

    return {'status': 'success', 'base58check_address': addresses['base58check_address']}

@api.post('/balance')
def get_balance():
    client = get_tron_client()
    contract_address = get_contract_address(g.symbol)
    contract = client.get_contract(contract_address)
    precision = contract.functions.decimals()
    balance = Decimal(0)
    for row in  query_db('select public from keys where symbol = ? and type = "onetime"', (g.symbol,)):
        balance += Decimal(contract.functions.balanceOf(row['public']) )
    balance = balance / 10 ** precision

    return {'status': 'success', 'balance': balance}

@api.post('/status')
def get_status():
    client = get_tron_client()
    block =  client.get_latest_block()
    return {'status': 'success', 'last_block_timestamp': block['block_header']['raw_data']['timestamp'] // 1000}

@api.post('/transaction/<txid>')
def get_transaction(txid):

    row = query_db('select * from events where txid = ?', (txid, ), one=True)
    event_data = json.loads(row['event'])

    rows = query_db('select public from keys where symbol = ?', (g.symbol, ))
    addrs = [row['public'] for row in rows]

    if event_data['topicMap']['to'] in addrs:
            category = 'receive'
            addr = event_data['topicMap']['to']

    elif event_data['topicMap']['from'] in addrs:
            category = 'send'
            addr = event_data['topicMap']['from']

    else:
        return {'status': 'error', 'msg': 'txid is not related to any known address'}

    client = get_tron_client()
    contract_address = get_contract_address(g.symbol)
    contract = client.get_contract(contract_address)
    precision = contract.functions.decimals()
    amount = Decimal(event_data['dataMap']['value']) / 10 ** precision
    confirmations = get_confirmations(txid)

    return {'address': addr, 'amount': amount, 'confirmations': confirmations, 'category': category}

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
