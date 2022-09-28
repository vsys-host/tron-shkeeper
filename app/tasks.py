import concurrent
import decimal

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
            return {'txid': txn.txid}

        except Exception as e:
            logger.exception(f"Error during transfer {spec['amount']} {symbol} {spec['src'].addr} -> {spec['dst']}: {e}")

    for step in steps:
        with concurrent.futures.ThreadPoolExecutor(max_workers=config['CONCURRENT_MAX_WORKERS']) as executor:
            return list(executor.map(transfer, step))

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
