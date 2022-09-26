import logging
from decimal import Decimal
from typing import Literal
import concurrent

import tronpy.exceptions
from flask import current_app
from tronpy import Tron
from tronpy.keys import PrivateKey
from tronpy.providers import HTTPProvider
from werkzeug.routing import BaseConverter

from .config import config, get_contract_address
from .db import get_db, query_db
from .logging import logger


class DecimalConverter(BaseConverter):

    def to_python(self, value):
        return Decimal(value)

    def to_url(self, value):
        return BaseConverter.to_url(value)


def get_filter_config():
    with current_app.app_context():
        return { row['public']: row['symbol']
                 for row in query_db('select public, symbol from keys where type = "onetime"') }


def get_symbol_by_addr(addr):
    with current_app.app_context():
        return query_db('select symbol from keys where public = ?', (addr), one=True)


def get_confirmations(txid):
    try:
        full_node = get_tron_client()
        latest_block_number = full_node.get_latest_block_number()
        tx_info = full_node.get_transaction_info(txid)
        confirmations = latest_block_number - tx_info['blockNumber']
        logger.debug(f"confirmations: {confirmations} = latest_block_number: {latest_block_number} - tx_info['blockNumber'] {tx_info['blockNumber']}")

    except tronpy.exceptions.TransactionNotFound:
        logger.exception('Exception in get_confirmations():')
        confirmations = 0

    return confirmations

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
                (addresses['base58check_address'], addresses['private_key']),
            )
            db.commit()
            logger.info('Fee deposit account has been created.')

def get_network_currency_balance(addr) -> Decimal:
    client = get_tron_client()
    try:
        return client.get_account_balance(addr)
    except tronpy.exceptions.AddressNotFound:
        return Decimal(0)

def get_token_balance(addr, symbol) -> Decimal:
    client = get_tron_client()
    contract_address = get_contract_address(symbol)
    contract = client.get_contract(contract_address)
    precision = contract.functions.decimals()
    balance =  Decimal(contract.functions.balanceOf(addr))
    return balance / 10 ** precision

def get_non_empty_accounts(symbol=None, filter: Literal['tokens','currency'] = 'tokens'):
    """Return a list of accounts having non empty token balance.

    Filter sets the balance type to check: tokens (default) or currency."""

    if symbol:
        rows = query_db('select public from keys where symbol = ? and type = "onetime"', (symbol, ))
    else:
        rows = query_db('select public from keys where type = "onetime"')

    accounts = []
    for row in rows:
        tokens = get_token_balance(row['public'], symbol) if symbol else Decimal(0)
        currency = get_network_currency_balance(row['public'])
        bandwidth = get_bandwidth(row['public'])
        if (filter == 'tokens' and tokens) or (filter == 'currency' and currency):
            accounts.append({
                'addr': row['public'],
                'token_balance': tokens,
                'network_currency_balance': currency,
                'bandwidth': bandwidth,
            })

    accounts.sort(key=lambda x: x['token_balance'], reverse=True)
    return accounts


def choose_accounts(amount: float, accounts: list):

    if amount <= 0:
        raise Exception(f'Invalid amount porivded: {amount}')

    single_address_has_amount = list(filter(lambda x: x['token_balance'] == amount, accounts))
    if single_address_has_amount:
        return [single_address_has_amount[0]]

    accounts_sum = sum(map(lambda x: x['token_balance'], accounts))
    if amount == accounts_sum:
        return accounts
    if amount > accounts_sum:
        raise Exception(f'Not enough tokens to pay out {amount}. Has only {accounts_sum}')

    choosed = []
    for account in accounts:
        choosed_sum = sum(map(lambda x: x['token_balance'], choosed))
        if choosed_sum == amount:
            return choosed
        if choosed_sum < amount:
            if account['token_balance'] > (amount - choosed_sum):
                account['orig_token_balance'] = account['token_balance']
                account['token_balance'] = amount - choosed_sum
                choosed.append(account)
            else:
                choosed.append(account)
    return choosed

def get_bandwidth(account):
    client = get_tron_client()
    try:
        resources = client.get_account_resource(account)
    except tronpy.exceptions.AddressNotFound:
        resources = {}
    bandwidth_limit = resources.get('freeNetLimit', 0)
    bandwidth_used = resources.get('freeNetUsed', 0)
    return {
        'limit': bandwidth_limit,
        'now': bandwidth_limit - bandwidth_used,
    }

def get_free_bandwidth_accounts(accounts):
    free_bandwidth_accounts = []
    for account in accounts:
        bw = get_bandwidth(account['addr'])
        logger.info(f'Account {account["addr"]} bandwidth: {bw["now"]} limit: {bw["limit"]}')
        # if bw['limit'] and :
        if 1:
            free_bandwidth_accounts.append(account)

    return free_bandwidth_accounts

def transfer_to_fee_deposit(accounts):
    """Send network currency from onetime accounts to fee-deposit account"""

    if not accounts:
        logger.info(f'Onetime accounts have no unused network currency to send back to fee-deposit account.')

    client = get_tron_client()
    fee_deposit_key = query_db('select * from keys where type = "fee_deposit" ', one=True)

    for account in accounts:
        onetime_account_keys = query_db('select * from keys where type = "onetime" and public = ?', (account['addr'],), one=True)
        priv_key = PrivateKey(bytes.fromhex(onetime_account_keys['private']))
        try:
            txn = (
                client.trx.transfer(account['addr'], fee_deposit_key['public'], int(account['network_currency_balance'] * 1_000_000))
                .build()
                .sign(priv_key)
            )
            txn.broadcast().wait()
            logger.info(f"TX {txn.txid} sent from: {account['addr']} to: {fee_deposit_key['public']} value: {account['network_currency_balance']}")
        except tronpy.exceptions.ValidationError as e:
            logger.info(f"Error while transferring to fee deposit account from {account['addr']}: {e}")


def seed_payout_fee(accounts):
    """Send network currency enought to make a transaction to each account"""

    client = get_tron_client()

    fee_deposit_key = query_db('select * from keys where type = "fee_deposit" ', one=True)
    priv_key = PrivateKey(bytes.fromhex(fee_deposit_key['private']))

    accounts_need_seeding = len(list(filter(lambda x: x['network_currency_balance'] < config['TX_FEE'], accounts)))
    fee_deposit_account_balance = client.get_account_balance(fee_deposit_key['public'])


    need_currency = accounts_need_seeding * config['TX_FEE']
    if fee_deposit_account_balance < need_currency:
        raise Exception(f'Fee deposit account has not enought currency. Has: {fee_deposit_account_balance} need: {need_currency}')

    results = []
    for account in accounts:
        if account['network_currency_balance'] >= config['TX_FEE']:
            logger.info(f"Skipping {account['addr']}: network currency balance is {account['network_currency_balance']}")
            continue

        txn = (
            client.trx.transfer(fee_deposit_key['public'], account['addr'], int(config['TX_FEE'] * 1_000_000))
            .build()
            .sign(priv_key)
        )
        txn.broadcast().wait()
        logger.info(f"TX {txn.txid} sent from: {fee_deposit_key['public']} to: {account['addr']} value: {config['TX_FEE']}")
        results.append({'addr': account['addr'], 'txid': txn.txid})
    return results

def send_payment(from_accs: list, to: str, symbol: str):
    result = []
    for account in from_accs:
        txid = transfer(account['addr'], to, account['token_balance'], symbol)
        result.append({
            'addr': account['addr'],
            'amount': account['token_balance'],
            'txid': txid,
        })
    return result

def transfer(acc_from, acc_to, amount, symbol):
    amount = Decimal(amount)
    client = get_tron_client()

    onetime_account_keys = query_db('select * from keys where type = "onetime" and public = ?', (acc_from,), one=True)
    priv_key = PrivateKey(bytes.fromhex(onetime_account_keys['private']))

    contract_address = get_contract_address(symbol)
    contract = client.get_contract(contract_address)
    txn = (
        contract.functions.transfer(acc_to, int(amount * 1_000_000))
        .with_owner(onetime_account_keys['public'])
        .fee_limit(int(config['TX_FEE_LIMIT'] * 1_000_000))
        .build()
        .sign(priv_key)
    )
    txn.broadcast().wait()
    logger.info(f'Transfered {amount} {symbol} from {acc_from} to {acc_to} with txid {txn.txid}')

    return txn.txid

def get_tron_client(node : Literal['full', 'solidity'] = 'full') -> Tron:
    provider = HTTPProvider(config['FULLNODE_URL'] if node == 'full'
                                                   else config['SOLIDITYNODE_URL'])
    provider.sess.auth = (config['TRON_NODE_USERNAME'] , config['TRON_NODE_PASSWORD'])
    return Tron(provider)

def get_wallet_balance(symbol) -> Decimal:
    client = get_tron_client()
    contract_address = get_contract_address(symbol)
    contract = client.get_contract(contract_address)
    precision = contract.functions.decimals()
    balance = Decimal(0)
    accounts = [row['public'] for row in query_db('select public from keys where symbol = ? and type = "onetime"', (symbol,))]
    with concurrent.futures.ThreadPoolExecutor(max_workers=config['CONCURRENT_MAX_WORKERS']) as executor:
        balance = sum(executor.map(lambda acc: Decimal(contract.functions.balanceOf(acc)), accounts)) / 10 ** precision
    return balance
