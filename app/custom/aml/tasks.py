import time
from sqlmodel import Session, select
from ... import celery
from ...config import config
from .classes import AmlWallet
from ...utils import short_txid

from app.db import engine, query_db
from app.logging import logger
from .models import Transaction
from app.schemas import TronAddress, TronSymbol
from app.utils import skip_if_running
from app.wallet import Wallet


@celery.task(bind=True)
@skip_if_running
def run_payout_for_tx(self, symbol, account, tx_id):
    wallet = AmlWallet(symbol=symbol)
    if account == wallet.main_account["public"]:
        logger.debug(f"{account} is fee-dopisit, skipping ")
        return False
    results = wallet.payout_for_tx(tx_id, account)
    return results


@celery.task(bind=True)
@skip_if_running
def check_transaction(self, symbol: TronSymbol, account: TronAddress, txid: str):
    from .functions import (
        aml_check_transaction,
    )

    result = aml_check_transaction(account, txid)
    if (
        result["result"]
        and result["data"]["status"] == "pending"
        and "uid" in result["data"]
    ):
        status = "rechecking"
        uid = result["data"]["uid"]
        score = -1
    elif (
        result["result"]
        and "riskscore" in result["data"]
        and "uid" in result["data"]
        and result["data"]["status"] == "success"
    ):
        status = "ready"
        score = result["data"]["riskscore"]
        uid = result["data"]["uid"]
    else:
        logger.warning(f"Cannot update the transaction, something wrong - {result}")
        return False

    time.sleep(5)

    with Session(engine) as session:
        pd = session.exec(
            select(Transaction).where(
                Transaction.address == account, Transaction.tx_id == txid
            )
        ).one()
        pd.uid = uid
        pd.score = score
        pd.status = status
        session.add(pd)
        session.commit()
        session.refresh(pd)

    if status == "ready":
        run_payout_for_tx.delay(symbol, account, txid)
        return True


@celery.task(bind=True)
@skip_if_running
def recheck_transaction(self, uid, txid):
    from .functions import (
        aml_recheck_transaction,
    )

    result = aml_recheck_transaction(uid, txid)
    if (
        result["result"]
        and result["data"]["status"] == "pending"
        and "uid" in result["data"]
    ):
        status = "rechecking"
        uid = result["data"]["uid"]
        score = -1
    elif (
        result["result"]
        and "riskscore" in result["data"]
        and "uid" in result["data"]
        and result["data"]["status"] == "success"
    ):
        status = "ready"
        score = result["data"]["riskscore"]
        uid = result["data"]["uid"]
    else:
        logger.warning(f"Cannot update the transaction, something wrong - {result}")
        return False

    with Session(engine) as session:
        pd = session.exec(select(Transaction).where(Transaction.tx_id == txid)).first()
        if not pd:
            logger.warning(f"Cannot find tx {short_txid(txid)} in DB")
            return False
        pd.uid = uid
        pd.score = score
        pd.status = status
        session.add(pd)
        session.commit()
        session.refresh(pd)

    if status == "ready":
        run_payout_for_tx.delay(pd.crypto, pd.address, txid)


@celery.task(bind=True)
@skip_if_running
def recheck_transactions(self):
    with Session(engine) as session:
        query_recheck = select(Transaction).where(
            Transaction.ttype == "aml", Transaction.status == "rechecking"
        )
        for tx in session.exec(query_recheck):
            recheck_transaction.delay(tx.uid, tx.tx_id)

        query_pending = select(Transaction).where(
            Transaction.ttype == "aml", Transaction.status == "pending"
        )
        for tx in session.exec(query_pending):
            check_transaction.delay(tx.crypto, tx.address, tx.tx_id)
    return True


@celery.task(bind=True)
@skip_if_running
def sweep_accounts(self):
    accounts = [
        row["public"]
        for row in query_db('SELECT public FROM keys WHERE type = "onetime"')
    ]
    logger.info(f"sweeping {len(accounts)} accounts")
    for account in accounts:
        try:
            #
            # TRC20
            #
            for symbol in [token.symbol for token in config.get_tokens()]:
                wallet = Wallet(symbol=symbol)
                balance = wallet.balance_of(account)
                if not balance:
                    continue
                if balance < config.get_min_transfer_threshold(symbol):
                    logger.info(
                        f"{account} balance {balance} {symbol} is less than minimal transfer"
                        f"threshold of {config.get_min_transfer_threshold(symbol)}, skip sweeping"
                    )
                    continue
                logger.info(f"{account} has balance {balance} {symbol.name}")
                with Session(engine) as session:
                    txs = session.exec(
                        select(Transaction).where(
                            Transaction.address == account,
                            Transaction.crypto == symbol,
                        )
                    ).all()
                    for tx in txs:
                        run_payout_for_tx.delay(symbol, account, tx.tx_id)

            #
            # TRX
            #
            symbol = "TRX"
            balance = Wallet().balance_of(account)
            if not balance:
                continue
            if balance < config.TRX_MIN_TRANSFER_THRESHOLD:
                logger.info(
                    f"{account} balance {balance} {symbol} is less than minimal transfer"
                    f"threshold of {config.TRX_MIN_TRANSFER_THRESHOLD}, skip sweeping"
                )
                continue
            logger.info(f"{account} has balance {balance} {symbol}")
            with Session(engine) as session:
                txs = session.exec(
                    select(Transaction).where(
                        Transaction.address == account,
                        Transaction.crypto == symbol,
                    )
                ).all()
                for tx in txs:
                    run_payout_for_tx.delay(symbol, account, tx.tx_id)

        except Exception as e:
            logger.exception(f"{account} sweep error: {e}")
