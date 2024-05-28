import collections
import concurrent
from contextlib import closing
import datetime
import decimal
from functools import lru_cache
import sqlite3
import time
from decimal import Decimal
from typing import Dict, List

from celery.schedules import crontab
from tronpy.keys import PrivateKey
from tronpy.tron import current_timestamp
import tronpy.exceptions
import requests

from . import celery
from .config import config, get_contract_address, get_min_transfer_threshold, get_internal_trc20_tx_fee
from .db import query_db, query_db2
from .wallet import Wallet
from .utils import skip_if_running
from .connection_manager import ConnectionManager
from .logging import logger
from .wallet_encryption import wallet_encryption


@celery.task()
def prepare_payout(dest, amount, symbol):
    if (balance := Wallet(symbol).balance) < amount:
        raise Exception(f"Wallet balance is less than payout amount: {balance} < {amount}")
    steps = []
    steps.append({
            'dst': dest,
            'amount': decimal.Decimal(amount),
    })
    return steps

@celery.task()
def prepare_multipayout(payout_list, symbol):
    logger.info(f"Preparing payout for {sum([t['amount'] for t in payout_list])} "
                f"{symbol} to {len(payout_list)} destinations.")
    steps = []
    for payout in payout_list:
        steps.append({
            'dst': payout['dest'],
            'amount': decimal.Decimal(payout['amount']),
        })
    return steps

@celery.task()
def payout(steps, symbol):
    wallet = Wallet(symbol)
    with concurrent.futures.ThreadPoolExecutor(max_workers=config['CONCURRENT_MAX_WORKERS']) as executor:
        payout_results = list(executor.map(lambda x: wallet.transfer(x['dst'], x['amount']), steps))
    post_payout_results.delay(payout_results, symbol)
    return payout_results

@celery.task()
def transfer_trc20_from(onetime_publ_key, symbol):
    '''
    Transfers TRC20 from onetime to main account
    '''

    tron_client = ConnectionManager.client()

    contract_address = get_contract_address(symbol)
    contract = tron_client.get_contract(contract_address)
    precision = contract.functions.decimals()
    token_balance = contract.functions.balanceOf(onetime_publ_key)

    min_threshold = get_min_transfer_threshold(symbol)
    balance = Decimal(token_balance) / 10**precision
    if balance <= min_threshold:
        logger.warning(f"Skipping transfer: account {onetime_publ_key} has only "
                       f"{balance} {symbol}. Threshold is {min_threshold} {symbol}")
        return

    logger.warning(f"Transfer to main acc started for {onetime_publ_key}. Balance: "
                    f"{balance} {symbol}. Threshold is {min_threshold} {symbol}")

    main_acc_keys = query_db2('select * from keys where type = "fee_deposit" ', one=True)
    main_priv_key = PrivateKey(bytes.fromhex(wallet_encryption.decrypt(main_acc_keys['private'])))
    main_publ_key = main_acc_keys['public']

    main_acc_balance = tron_client.get_account_balance(main_publ_key)

    if main_acc_balance < get_internal_trc20_tx_fee():
        raise Exception(f"Main account hasn't enought currency: balance: {main_acc_balance} need: {get_internal_trc20_tx_fee()}")

    tx_trx = tron_client.trx.transfer(main_publ_key, onetime_publ_key, int(get_internal_trc20_tx_fee() * 1_000_000))
    tx_trx._raw_data['expiration'] = current_timestamp() + 60_000
    tx_trx = tx_trx.build()
    tx_trx = tx_trx.sign(main_priv_key)
    tx_trx_res = tx_trx.broadcast().wait()
    logger.info(f"Fee sent to {onetime_publ_key} with TXID {tx_trx.txid}. Details: {tx_trx_res}")

    onetime_priv_key = PrivateKey(bytes.fromhex(wallet_encryption.decrypt(query_db2('select * from keys where type = "onetime" and public = ?', (onetime_publ_key,), one=True)['private'])))

    tx_token = contract.functions.transfer(main_publ_key, int(token_balance))
    tx_token = tx_token.with_owner(onetime_publ_key)
    tx_token = tx_token.fee_limit(int(config['TX_FEE_LIMIT'] * 1_000_000))
    tx_token._raw_data['expiration'] = current_timestamp() + 60_000
    tx_token = tx_token.build()
    tx_token = tx_token.sign(onetime_priv_key)
    tx_token_res = tx_token.broadcast().wait()
    logger.info(f"{token_balance / 10**precision} {symbol} sent to {onetime_publ_key} with {tx_token.txid}. Details: {tx_token_res}")

    return {'tx_trx_res': tx_trx_res, 'tx_token': tx_token_res}

@celery.task()
def transfer_trx_from(onetime_publ_key):
    '''
    Transfers TRX from onetime to main account
    '''

    tron_client = ConnectionManager.client()
    onetime_priv_key = PrivateKey(bytes.fromhex(wallet_encryption.decrypt(query_db2('select * from keys where type = "onetime" and public = ?', (onetime_publ_key,), one=True)['private'])))

    onetime_acc_balance = tron_client.get_account_balance(onetime_publ_key)
    if onetime_acc_balance == 0:
        return {'status':'error','error':'skipping 0 TRX account'}

    main_publ_key = query_db2('select * from keys where type = "fee_deposit" ', one=True)['public']

    tx_trx = tron_client.trx.transfer(onetime_publ_key, main_publ_key, int(onetime_acc_balance * 1_000_000))
    tx_trx._raw_data['expiration'] = current_timestamp() + 60_000
    tx_trx = tx_trx.build()
    tx_trx = tx_trx.sign(onetime_priv_key)
    tx_trx_res = tx_trx.broadcast().wait()
    logger.info(f"{onetime_acc_balance} TRX sent to main account ({main_publ_key}) with TXID {tx_trx.txid}. Details: {tx_trx_res}")
    return {'tx_trx_res': tx_trx_res}

@celery.task()
def post_payout_results(data, symbol):
    while True:
        try:
            return requests.post(
                f'http://{config["SHKEEPER_HOST"]}/api/v1/payoutnotify/{symbol}',
                headers={'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']},
                json=data,
            )
        except Exception as e:
            logger.warning(f'Shkeeper payout notification failed: {e}')
            time.sleep(10)


def is_task_running(task_instance, name: str, args: List = None, kwargs: Dict = None):
    workers = task_instance.app.control.inspect().active()
    for worker, tasks in workers.items():
        for task in tasks:
            # check if task name matches
            if task['name'] != name:
                continue
            # check if args is subset of task args
            if args and not (set(args) <= set(task['args'])):
                continue
            # check if kwargs is subset of task kwargs
            if kwargs and not (kwargs.items() <= task['kwargs'] .items()):
                continue
            return True
    return False


@celery.task(bind=True)
@skip_if_running
def scan_accounts(self, *args, **kwargs):
    '''
    Scans onetime accounts balances (trc20, trx),
    saves it to database and transfers to main account.
    '''

    stats = {
        'balances': collections.defaultdict(Decimal),
        'exception_num': 0,
    }

    @lru_cache(maxsize=len(config['TOKENS'][config['TRON_NETWORK']]))
    def precision_of(symbol):
        return ConnectionManager.client().get_contract(get_contract_address(symbol)).functions.decimals()

    accounts = [row['public'] for row in query_db('SELECT public FROM keys WHERE type = "onetime"')]
    for index, account in enumerate(accounts, start=1):
        try:

            #
            # TRC20
            #

            for symbol in config['TOKENS'][config['TRON_NETWORK']]:
                contract = ConnectionManager.client().get_contract(get_contract_address(symbol))

                while ret:=0 < config['CONCURRENT_MAX_RETRIES']:
                    try:
                        trc20_balance = Decimal(contract.functions.balanceOf(account)) / 10 ** precision_of(symbol)
                        break
                    except tronpy.exceptions.UnknownError as e:
                        logger.debug(f'{account} {symbol} trc20 balance fetch error: {e}')
                        ret += 1
                else:
                    raise Exception(f'CONCURRENT_MAX_RETRIES reached while getting trc20 balance of {account}')

                stats['balances'][symbol] += trc20_balance

                if config['SAVE_BALANCES_TO_DB']:
                    with closing(sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)) as conn:
                        with conn: # as transaction
                            cur = conn.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = ?",
                                               (trc20_balance, datetime.datetime.now(), account, symbol))
                            if not cur.rowcount: # update failed
                                conn.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                             (account, symbol, trc20_balance, datetime.datetime.now()))

                if trc20_balance > 0:
                    if not is_task_running(self, 'app.tasks.transfer_trc20_from', args=[account, symbol]):
                        transfer_trc20_from(account, symbol)

            #
            # TRX
            #

            while ret:=0 < config['CONCURRENT_MAX_RETRIES']:
                try:
                    trx_balance = ConnectionManager.client().get_account_balance(account)
                    break
                except tronpy.exceptions.AddressNotFound:
                    trx_balance = Decimal(0)
                    break
                except tronpy.exceptions.UnknownError as e:
                    logger.debug(f'{account} TRX balance fetch error: {e}')
                    ret += 1
            else:
                raise Exception(f'CONCURRENT_MAX_RETRIES reached while getting TRX balance of {account}')

            stats['balances']['TRX'] += trx_balance

            if config['SAVE_BALANCES_TO_DB']:
                with closing(sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)) as conn:
                    with conn: # as transaction
                        cur = conn.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = 'TRX'",
                                           (trx_balance, datetime.datetime.now(), account))
                        if not cur.rowcount: # update failed
                            conn.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                         (account, 'TRX', trx_balance, datetime.datetime.now()))

            if trx_balance > 0:
                if not is_task_running(self, 'app.tasks.transfer_trc20_from', args=[account]):
                    # We don't need to check if account has a free bandwidth because tx will raise tronpy.exceptions.ValidationError
                    # if there is not enough TRX to burn for bandwidth. We are sending the entire TRX balance,
                    # so there will be no TRX to burn for sure.
                    transfer_trx_from(account)

            logger.debug(f"Scanned {index} of {len(accounts)} accounts, found: " + ", ".join([f"{v} {k}" for k,v in stats['balances'].items()]))

        except Exception as e:
            logger.debug(f"{account} scan error: {e}")
            stats['exception_num'] += 1

    return stats

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(config['BALANCES_RESCAN_PERIOD'], scan_accounts.s())