from decimal import Decimal

from flask import g

from .. import celery
from ..config import config
from ..db import query_db
from ..tasks import payout as payout_task
from ..tasks import prepare_payout as prepare_payout_task
from ..tasks import transfer_unused_fee
from ..utils import choose_accounts, get_non_empty_accounts
from . import api


@api.post('/calc-tx-fee/<decimal:amount>')
def calc_tx_fee(amount):
    accounts = get_non_empty_accounts(g.symbol)
    choosen_accounts = choose_accounts(amount, accounts)
    accounts_num = len(choosen_accounts)
    activation_and_transfer_fee = 2
    fee = accounts_num * (config['TX_FEE'] + activation_and_transfer_fee)
    return {
        'accounts_num': accounts_num,
        'fee': fee,
    }

@api.post('/payout/<to>/<decimal:amount>')
def payout(to, amount):
    # run payout_task(prepare_payout_task(...), ...) if prepare_payout_task exited successfully
    task = (
        prepare_payout_task.s(amount, g.symbol) | payout_task.s(to, g.symbol)
    ).apply_async()
    # return {'prepare_payout_task_id': task.id, 'payout_task_id': task.children[0].id}
    return {'task_id': task.id}

@api.post('/task/<id>')
def get_task(id):
    task = celery.AsyncResult(id)
    return {'status': task.status, 'result': task.result}

@api.post('/transfer-back')
def transfer_back():
    task = transfer_unused_fee.delay()
    return {'task_id': task.id}

@api.post('/balances/<type>')
def get_balances(type='tokens'):
    return {'accounts': get_non_empty_accounts(g.symbol, filter=type)}
