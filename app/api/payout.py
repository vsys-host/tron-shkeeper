from decimal import Decimal

from flask import g, request
from flask import current_app as app
import tronpy

from .. import celery
from ..tasks import payout as payout_task
from ..tasks import prepare_payout, prepare_multipayout
from ..tasks import transfer_unused_fee
from ..utils import get_non_empty_accounts
from . import api
from ..trc20wallet import PayoutStrategy, Trc20Wallet
from ..logging import logger


@api.post('/calc-tx-fee/<decimal:amount>')
def calc_tx_fee(amount):
    w = Trc20Wallet(g.symbol)
    payout_list = [{'dest': 'calc-tx-fee', 'amount': amount}]
    ps = PayoutStrategy(w, payout_list)
    return ps.estimate_fee()

@api.post('/multipayout')
def multipayout():
    try:
        payout_list = request.get_json(force=True)
    except Exception as e:
        raise Exception(f"Bad JSON in payout list: {e}")

    if not payout_list:
            raise Exception(f"Payout list is empty!")

    for transfer in payout_list:
        try:
            tronpy.keys.to_base58check_address(transfer['dest'])
        except Exception as e:
            raise Exception(f"Bad destination address in {transfer}: {e}")
        try:
            transfer['amount'] = Decimal(transfer['amount'])
        except Exception as e:
            raise Exception(f"Bad amount in {transfer}: {e}")

        if transfer['amount'] <= 0:
            raise Exception(f"Payout amount should be a positive number: {transfer}")

    wallet = Trc20Wallet(g.symbol)

    need_tokens = sum([transfer['amount'] for transfer in payout_list])
    if wallet.tokens < need_tokens:
        raise Exception(f"Not enough {g.symbol} tokens to make all payouts. Has: {wallet.tokens}, need: {need_tokens}")

    ps = PayoutStrategy(wallet, payout_list)
    need_currency = ps.estimate_fee()['fee']
    if wallet.fee_deposit_account.currency < need_currency:
        raise Exception(f"Not enough TRX tokens at fee-deposit account {wallet.fee_deposit_account.addr} to pay for payout fees. "
                        f"Has: {wallet.fee_deposit_account.currency}, need: {need_currency}")

    if 'dryrun' in request.args:
        return {
            'currency': {
                'need': need_currency,
                'have': wallet.fee_deposit_account.currency,
            },
            'tokens': {
                'need': need_tokens,
                'have': wallet.tokens,
            },
            'steps': ps.steps,
        }

    task = ( prepare_multipayout.s(payout_list, g.symbol) | payout_task.s(g.symbol) ).apply_async()
    return {'task_id': task.id}

@api.post('/payout/<to>/<decimal:amount>')
def payout(to, amount):
    task = (
        prepare_payout.s(to, amount, g.symbol) | payout_task.s(g.symbol)
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
