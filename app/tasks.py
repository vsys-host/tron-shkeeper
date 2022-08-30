import decimal
import time

from celery.schedules import crontab
from celery.utils.log import get_task_logger

from . import celery
from .utils import (choose_accounts, get_free_bandwidth_accounts,
                    get_non_empty_accounts, seed_payout_fee, send_payment,
                    transfer_to_fee_deposit)


logger = get_task_logger(__name__)

@celery.task()
def prepare_payout(amount, symbol):
    logger.info(f"Preparing payout for {amount} {symbol}")
    amount = decimal.Decimal(amount)
    accounts = get_non_empty_accounts(symbol)
    choosen_accounts = choose_accounts(amount, accounts)
    logger.info(f'Seeding fees to: {choosen_accounts}')
    seed_payout_fee(choosen_accounts)
    return choosen_accounts

@celery.task()
def payout(from_accs, to_acc, symbol):
    time.sleep(10)
    result = send_payment(from_accs, to_acc, symbol)
    return result

@celery.task()
def transfer_unused_fee():
    # We don't need to check if accounts have a free bandwidth units
    # because tx will raise tronpy.exceptions.ValidationError
    # if there is not enough TRX to burn for bandwidth.
    #
    # We are sending the entire TRX balance,
    # so there will be no TRX to burn for sure.
    transfer_to_fee_deposit(get_non_empty_accounts(filter='currency'))

@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    sender.add_periodic_task(
        crontab(hour=0, minute=0),
        transfer_unused_fee.s(),
    )