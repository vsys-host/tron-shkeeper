import json
from decimal import Decimal
import time
import requests

import tronpy.exceptions
from flask import current_app, g
from tronpy import Tron

from ..db import get_db, query_db
from ..utils import estimateenergy
from ..logging import logger
from ..wallet import Wallet
from ..block_scanner import BlockScanner, parse_tx
from ..connection_manager import ConnectionManager
from . import api
from ..wallet_encryption import wallet_encryption


@api.post("/generate-address")
def generate_new_address():
    client = Tron()
    addresses = client.generate_address()

    db = get_db()
    db.execute(
        "INSERT INTO keys (symbol, public, private, type) VALUES (?, ?, ?, 'onetime')",
        (
            g.symbol,
            addresses["base58check_address"],
            wallet_encryption.encrypt(addresses["private_key"]),
        ),
    )
    db.commit()

    BlockScanner.add_watched_account(addresses["base58check_address"])

    return {
        "status": "success",
        "base58check_address": addresses["base58check_address"],
    }


@api.post("/balance")
def get_balance():
    start = time.time()

    w = Wallet(g.symbol)
    balance = w.balance
    return {
        "status": "success",
        "balance": balance,
        "query_time": time.time() - start,
    }


@api.post("/status")
def get_status():
    bs = BlockScanner()
    last_seen_block_num = bs.get_last_seen_block_num()
    block = ConnectionManager.client().get_block(last_seen_block_num)
    return {
        "status": "success",
        "last_block_timestamp": block["block_header"]["raw_data"]["timestamp"] // 1000,
    }


@api.post("/transaction/<txid>")
def get_transaction(txid):
    tron_client = ConnectionManager.client()
    tx = tron_client.get_transaction(txid)
    tx_info = tron_client.get_transaction_info(txid)
    try:
        latest_block_number = tron_client.get_latest_block_number()
        tx_block_number = tx_info["blockNumber"]
        confirmations = latest_block_number - tx_block_number or 1
    except tronpy.exceptions.TransactionNotFound:
        logger.warning(f"Can't get confirmations for {txid}")
        confirmations = 1
    tron_tx_list = parse_tx(tx, tx_info)
    return [
        {
            "address": info.dst_addr,
            "amount": info.amount,
            "confirmations": confirmations,
            "category": "receive",
        }
        for info in tron_tx_list
        if info.dst_addr in BlockScanner.get_watched_accounts()
    ]


@api.post("/dump")
def dump():
    rows = query_db(
        'select * from keys where symbol = ? or type != "one_time"', (g.symbol,)
    )
    keys = []
    for row in rows:
        keys.append(
            {
                "public": row["public"],
                "private": wallet_encryption.decrypt(row["private"]),
                "type": row["type"],
                "symbol": row["symbol"],
            }
        )
    return {"accounts": keys}


@api.get("/addresses")
def list_addresses():
    rows = query_db(
        'select public from keys where symbol = ? or type = "fee_deposit"', (g.symbol,)
    )
    return {"accounts": [row["public"] for row in rows]}


@api.post("/fee-deposit-account")
def get_fee_deposit_account():
    client = ConnectionManager.client()
    key = query_db('select * from keys where type = "fee_deposit"', one=True)
    try:
        balance = client.get_account_balance(key["public"])
    except tronpy.exceptions.AddressNotFound:
        balance = Decimal(0)
    return {"account": key["public"], "balance": balance}


@api.post("/estimate-energy/<src>/<dst>/<decimal:amount>")
def estimate_energy(src, dst, amount):
    res = estimateenergy(src, dst, amount, g.symbol)
    logger.warning(f"estimateenergy result: {res}")
    return res


#
# Multiserver
#


@api.get("/multiserver/status")
def get_multiserver_status():
    statuses = ConnectionManager.manager().get_servers_status()
    return {"statuses": statuses}


@api.post("/multiserver/change/<int:server_id>")
def multiserver_change_server(server_id):
    if server_id in range(len(ConnectionManager.manager().servers)):
        ConnectionManager.manager().set_current_server_id(server_id)
        return {"status": "success", "msg": f"Changing server to {server_id}"}
    else:
        return {
            "status": "error",
            "msg": f"Can't change server to {server_id}: no such server in {ConnectionManager.manager().servers}",
        }


@api.post("/multiserver/switch-to-best")
def multiserver_switch_to_best():
    if ConnectionManager.manager().refresh_best_server():
        return {
            "status": "success",
            "msg": f"Changing server to {ConnectionManager.manager().get_current_server_id()}",
        }
    else:
        return {
            "status": "success",
            "msg": f"Server {ConnectionManager.manager().get_current_server_id()} is already the best server",
        }
