import concurrent
import datetime
import decimal
import sqlite3

from celery.schedules import crontab
from celery.utils.log import get_task_logger
from tronpy.keys import PrivateKey

from . import celery
from .config import config, get_contract_address
from .trc20wallet import PayoutStrategy, Trc20Wallet
from .utils import (get_non_empty_accounts, get_tron_client, transfer_to_fee_deposit)

logger = get_task_logger(__name__)

@celery.task()
def prepare_payout(dest, amount, symbol):
    logger.info(f"Preparing payout for {amount} {symbol} -> {dest}")
    amount = decimal.Decimal(amount)
    ps = PayoutStrategy(Trc20Wallet(symbol), [{'dest': dest, 'amount': amount}])
    steps = ps.generate_steps()
    seed_results = ps.seed_payout_fees()
    return steps

@celery.task()
def prepare_multipayout(payout_list, symbol):
    wallet = Trc20Wallet(symbol)
    ps = PayoutStrategy(wallet, payout_list)
    logger.info(f"Preparing payout for {sum([t['amount'] for t in payout_list])} "
                f"{symbol} to {len(payout_list)} destinations.")
    steps = ps.generate_steps()
    seed_results = ps.seed_payout_fees()
    return steps

@celery.task()
def payout(steps, symbol):

    client = get_tron_client()
    contract_address = get_contract_address(symbol)
    contract = client.get_contract(contract_address)

    def transfer(spec):
        try:
            txn = (
                contract.functions.transfer(spec['dst'], int(spec['amount'] * 1_000_000))
                .with_owner(spec['src'].addr)
                .fee_limit(int(config['TX_FEE_LIMIT'] * 1_000_000))
                .build()
                .sign(PrivateKey(bytes.fromhex(spec['src'].private_key)))
            )
            txn.broadcast().wait()
            logger.info(f"Transfer {spec['amount']} {symbol} {spec['src'].addr} -> {spec['dst']} | {txn.txid}")
            return txn.txid

        except Exception as e:
            logger.exception(f"Error during transfer {spec['amount']} {symbol} {spec['src'].addr} -> {spec['dst']}: {e}")

    payout_results = []
    for step in steps:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config['CONCURRENT_MAX_WORKERS']) as executor:
            txids = list(executor.map(transfer, step))
            payout_results.append({
                "dest": step[0]['dst'],
                "amount": sum([t['amount'] for t in step]),
                "status": "success",
                "txids": txids,
            })
    return payout_results

@celery.task()
def refresh_trc20_balances(symbol):
    con = sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
    with con:
        cur = con.cursor()
        cur.execute('PRAGMA journal_mode=wal')
        try:
            cur.execute('BEGIN IMMEDIATE')
        except sqlite3.OperationalError as e:
            logger.error(f"{config['BALANCES_DATABASE']} error: {e}")
            return e

        w = Trc20Wallet(symbol, init=False)
        updated = 0
        for acc in w.refresh_accounts():
            try:
                # tokens
                if cur.execute("SELECT * FROM trc20balances WHERE account = ? and symbol = ?", (acc.addr, symbol)).fetchone():
                    cur.execute("UPDATE trc20balances SET balance = ?, updated_at = ? WHERE account = ? AND symbol = ?",
                                (acc.tokens, datetime.datetime.now(), acc.addr, symbol))
                else:
                    cur.execute("INSERT INTO trc20balances VALUES (?, ?, ?, ?)",
                                (acc.addr, symbol, acc.tokens, datetime.datetime.now()))

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

@celery.task()
def transfer_unused_fee():
    # We don't need to check if accounts have a free bandwidth units
    # because tx will raise tronpy.exceptions.ValidationError
    # if there is not enough TRX to burn for bandwidth.
    #
    # We are sending the entire TRX balance,
    # so there will be no TRX to burn for sure.
    transfer_to_fee_deposit(get_non_empty_accounts(fltr='currency'))

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=0, minute=0),
        transfer_unused_fee.s(),
    )

    # Update USDT balances
    sender.add_periodic_task(config['UPDATE_TOKEN_BALANCES_EVERY_SECONDS'], refresh_trc20_balances.s('USDT'))
