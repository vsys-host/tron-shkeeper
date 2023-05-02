import concurrent
import datetime
import decimal
import sqlite3
import time
from decimal import Decimal

from celery.schedules import crontab
from tronpy.keys import PrivateKey
from tronpy.tron import current_timestamp
import requests

from . import celery
from .config import config, get_contract_address, get_min_transfer_threshold
from .db import query_db, query_db2
from .wallet import Wallet
from .trc20wallet import PayoutStrategy, Trc20Wallet
from .utils import get_non_empty_accounts, transfer_to_fee_deposit, Account, skip_if_running
from .connection_manager import ConnectionManager
from .logging import logger


@celery.task()
def prepare_payout(dest, amount, symbol):
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
def transfer_trc20_tokens_to_main_account(onetime_publ_key, symbol):

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
    main_priv_key = PrivateKey(bytes.fromhex(main_acc_keys['private']))
    main_publ_key = main_acc_keys['public']

    main_acc_balance = tron_client.get_account_balance(main_publ_key)

    if main_acc_balance < config['TX_FEE']:
        raise Exception(f"Main account hasn't enought currency: balance: {main_acc_balance} need: {config['TX_FEE']}")

    tx_trx = tron_client.trx.transfer(main_publ_key, onetime_publ_key, int(config['TX_FEE'] * 1_000_000))
    tx_trx._raw_data['expiration'] = current_timestamp() + 60_000
    tx_trx = tx_trx.build()
    tx_trx = tx_trx.sign(main_priv_key)
    tx_trx_res = tx_trx.broadcast().wait()
    logger.info(f"Fee sent to {onetime_publ_key} with TXID {tx_trx.txid}. Details: {tx_trx_res}")

    onetime_priv_key = PrivateKey(bytes.fromhex(query_db2('select * from keys where type = "onetime" and public = ?', (onetime_publ_key,), one=True)['private']))

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
def transfer_trx_to_main_account(onetime_publ_key):
    tron_client = ConnectionManager.client()
    onetime_priv_key = PrivateKey(bytes.fromhex(query_db2('select * from keys where type = "onetime" and public = ?', (onetime_publ_key,), one=True)['private']))

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

@celery.task(bind=True)
@skip_if_running
def refresh_trc20_balances(self, symbol):

    w = Trc20Wallet(symbol, init=False)
    accs = w.refresh_accounts()

    con = sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
    con.execute('pragma journal_mode=wal')
    with con:
        cur = con.cursor()
        try:
            cur.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as e:
            logger.error(f"{config['BALANCES_DATABASE']} error: {e}")
            return e

        updated = 0
        for acc in accs:
            try:
                # tokens
                if cur.execute("SELECT * FROM trc20balances WHERE account = ? and symbol = ?", (acc.addr, symbol)).fetchone():
                    cur.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = ?",
                                (acc.tokens, datetime.datetime.now(), acc.addr, symbol))
                else:
                    cur.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                (acc.addr, symbol, acc.tokens, datetime.datetime.now()))

                if acc.tokens > 0:
                    transfer_trc20_tokens_to_main_account.delay(acc.addr, symbol)

                # currency
                if cur.execute("SELECT * FROM trc20balances WHERE account = ? and symbol = ?", (acc.addr, '_currency')).fetchone():
                    cur.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = ?",
                                (acc.currency, datetime.datetime.now(), acc.addr, '_currency'))
                else:
                    cur.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                (acc.addr, '_currency', acc.currency, datetime.datetime.now()))

                # bandwidth
                if cur.execute("SELECT * FROM trc20balances WHERE account = ? and symbol = ?", (acc.addr, '_bandwidth')).fetchone():
                    cur.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = ?",
                                (acc.bandwidth, datetime.datetime.now(), acc.addr, '_bandwidth'))
                else:
                    cur.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                (acc.addr, '_bandwidth', acc.bandwidth, datetime.datetime.now()))

                updated += 1
            except Exception as e:
                logger.exception(f'Exception while updating {symbol} balance for {acc.addr}: {e}')
    con.close()
    return updated

@celery.task(bind=True)
def transfer_unused_fee(self):
    # We don't need to check if accounts have a free bandwidth units
    # because tx will raise tronpy.exceptions.ValidationError
    # if there is not enough TRX to burn for bandwidth.
    #
    # We are sending the entire TRX balance,
    # so there will be no TRX to burn for sure.
    rows = query_db('select public from keys where type = "onetime"')
    for row in rows:
        try:
            res = transfer_trx_to_main_account(row['public'])
            logger.warning(f'{row["public"]} -> main transfer result: {res}')
        except Exception as e:
            logger.warning(f'{row["public"]} -> main transfer error: {e}')

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=0, minute=0),
        transfer_unused_fee.s(),
    )

    # Update cached account balances
    for symbol in config['TOKENS'][config['TRON_NETWORK']]:
        sender.add_periodic_task(config['UPDATE_TOKEN_BALANCES_EVERY_SECONDS'], refresh_trc20_balances.s(symbol))
