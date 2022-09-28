
from flask import g

from .. import celery
from ..tasks import payout as payout_task
from ..tasks import prepare_payout as prepare_payout_task
from ..tasks import transfer_unused_fee
from ..utils import get_non_empty_accounts
from . import api
from ..trc20wallet import PayoutStrategy, Trc20Wallet


@api.post('/calc-tx-fee/<decimal:amount>')
def calc_tx_fee(amount):
    w = Trc20Wallet(g.symbol)
    payout_list = [{'dest': 'calc-tx-fee', 'amount': amount}]
    ps = PayoutStrategy(w, payout_list)
    return ps.estimate_fee()

@api.post('/payout/<to>/<decimal:amount>')
def payout(to, amount):
    task = (
        prepare_payout_task.s(to, amount, g.symbol) | payout_task.s(g.symbol)
    ).apply_async()
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
    return {'accounts': get_non_empty_accounts(g.symbol, fltr=type)}
