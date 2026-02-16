from decimal import Decimal

from flask import g, request
from flask import current_app as app
import tronpy
from pydantic import ValidationError


from .. import celery
from ..config import config
from ..tasks import custom_aml2_payout, payout as payout_task
from ..tasks import prepare_payout, prepare_multipayout
from . import api
from ..wallet import Wallet
from ..logging import logger
from ..schemas import CustomAml2MultipayoutList


@api.post("/calc-tx-fee/<decimal:amount>")
def calc_tx_fee(amount):
    return {"fee": config.TX_FEE}


@api.post("/multipayout")
def multipayout():
    try:
        payout_list = request.get_json(force=True)
    except Exception as e:
        raise Exception(f"Bad JSON in payout list: {e}")

    if not payout_list:
        raise Exception(f"Payout list is empty!")

    for transfer in payout_list:
        try:
            tronpy.keys.to_base58check_address(transfer["dest"])
        except Exception as e:
            raise Exception(f"Bad destination address in {transfer}: {e}")
        try:
            transfer["amount"] = Decimal(transfer["amount"])
        except Exception as e:
            raise Exception(f"Bad amount in {transfer}: {e}")

        if transfer["amount"] <= 0:
            raise Exception(f"Payout amount should be a positive number: {transfer}")

    wallet = Wallet(g.symbol)
    balance = wallet.balance
    need_tokens = sum([transfer["amount"] for transfer in payout_list])
    if balance < need_tokens:
        pass
        # raise Exception(f"Not enough {g.symbol} tokens to make all payouts. Has: {balance}, need: {need_tokens}")

    need_currency = len(payout_list) * config.TX_FEE
    trx_balance = Wallet().balance
    if trx_balance < need_currency:
        raise Exception(
            f"Not enough TRX tokens at fee-deposit account {wallet.main_account} to pay payout fees. "
            f"Has: {trx_balance}, need: {need_currency}"
        )

    if "dryrun" in request.args:
        return {
            "currency": {
                "need": need_currency,
                "have": trx_balance,
            },
            "tokens": {
                "need": need_tokens,
                "have": balance,
            },
        }

    task = (
        prepare_multipayout.s(payout_list, g.symbol) | payout_task.s(g.symbol)
    ).apply_async()
    return {"task_id": task.id}


@api.post("/payout/<to>/<decimal:amount>")
def payout(to, amount):
    task = (
        prepare_payout.s(to, amount, g.symbol) | payout_task.s(g.symbol)
    ).apply_async()
    return {"task_id": task.id}


@api.post("/task/<id>")
def get_task(id):
    task = celery.AsyncResult(id)
    if isinstance(task.result, Exception):
        return {"status": task.status, "result": task.result.args[0]}
    else:
        return {"status": task.status, "result": task.result}


#
# Custom AML2
#


@api.post("/custom_aml2_multipayout")
def custom_aml2_multipayout():
    try:
        payout_data = request.get_json(force=True)
    except Exception as e:
        raise Exception(f"Bad JSON in payout list: {e}")

    try:
        payout_list = CustomAml2MultipayoutList.validate_python(payout_data)
    except ValidationError as e:
        raise Exception(f"Invalid payout list: {e}")

    print(payout_list)

    task = (custom_aml2_payout.s(payout_list, g.symbol)).apply_async()
    return {"task_id": task.id}
