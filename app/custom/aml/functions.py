from decimal import Decimal
import hashlib
from typing import List, Literal
import requests
from sqlmodel import Session, select

from ...custom.aml.tasks import (
    check_transaction,
)

from ...schemas import TronSymbol, TronAddress
from .models import Transaction, Payout
from ...db import engine
from ...logging import logger
from ...config import config
from ...utils import short_txid


def add_transaction_to_db(hash, account, amount, symbol, internal_type=False):
    logger.info("Adding tx to DB")
    drain_type = get_external_drain_type(symbol)
    status = ""
    if internal_type:
        if internal_type == "from_fee":
            ttype = "from_fee"
            status = "skipped"
            score = -1
    elif not drain_type:
        raise Exception(f"Can't get payout type for tx {hash}")
    elif drain_type == "aml":
        if amount > get_min_check_amount(symbol):
            check_transaction.delay(symbol, account, hash)
            ttype = "aml"
            status = "pending"
            score = -1
        else:
            logger.warning(
                "Transaction amount is lower than min check amount in config. Adding it with max score"
            )
            score = 1
    elif drain_type == "regular":
        ttype = "regular"
        status = "pending"
        score = -1
    else:
        logger.warning("Type is undefined")
        return False

    with Session(engine) as session:
        session.add(
            Transaction(
                tx_id=hash,
                status=status,
                ttype=ttype,
                crypto=symbol,
                score=score,
                amount=amount,
                address=account,
            )
        )
        session.commit()


def get_min_check_amount(symbol: TronSymbol) -> Decimal:
    return config.EXTERNAL_DRAIN_CONFIG.aml_check.cryptos[symbol].min_check_amount


def get_external_drain_type(symbol: TronSymbol) -> Literal["aml", "regular"]:
    if (
        config.EXTERNAL_DRAIN_CONFIG.aml_check.state == "enabled"
        and symbol in config.EXTERNAL_DRAIN_CONFIG.aml_check.cryptos
    ):
        return "aml"

    elif (
        config.EXTERNAL_DRAIN_CONFIG.regular_split.state == "enabled"
        and symbol in config.EXTERNAL_DRAIN_CONFIG.regular_split.cryptos
    ):
        return "regular"
    else:
        raise Exception(f"Can't get payout type for {symbol}")


def aml_check_transaction(address, txid):
    symbol = "TRX"
    token_string = f"{txid}:{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_key}:{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_id}"
    token = str(hashlib.md5(token_string.encode()).hexdigest())
    response = requests.post(
        f"{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_point}/",
        data={
            "hash": txid,
            "address": address,
            "asset": symbol,
            "direction": "deposit",
            "token": token,
            "accessId": config.EXTERNAL_DRAIN_CONFIG.aml_check.access_id,
            "locale": "en_US",
            "flow": config.EXTERNAL_DRAIN_CONFIG.aml_check.flow,
        },
    )
    response.raise_for_status()
    return response.json()


def aml_recheck_transaction(uid, txid):
    token_string = f"{txid}:{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_key}:{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_id}"
    token = str(hashlib.md5(token_string.encode()).hexdigest())
    payload = f"uid={uid}&accessId={config.EXTERNAL_DRAIN_CONFIG.aml_check.access_id}&token={token}"
    headers = {}
    response = requests.post(
        f"{config.EXTERNAL_DRAIN_CONFIG.aml_check.access_point}/recheck",
        headers=headers,
        data=payload,
    )
    response.raise_for_status()
    return response.json()


def build_payout_list(
    symbol: TronSymbol, tx_id: str
) -> List[tuple[TronAddress, Decimal, Decimal]] | Literal[False]:
    external_drain_list = []
    addresses_done = []

    with Session(engine) as session:
        transaction = session.exec(
            select(Transaction).where(Transaction.tx_id == tx_id)
        ).first()

    if not transaction:
        logger.warning(f"Cannot find transaction {short_txid(tx_id)} in database")
        return False

    with Session(engine) as session:
        pd = session.exec(select(Payout).where(Payout.tx_id == tx_id)).all()

    for drain in pd:
        addresses_done.append(drain.address)

    if transaction.ttype == "from_fee":
        return False

    payout_type = get_external_drain_type(symbol)

    if "aml" == payout_type:
        if transaction.ttype == "aml" and transaction.status == "ready":
            risk_config = config.EXTERNAL_DRAIN_CONFIG.aml_check.cryptos[symbol]
            external_amounts = Decimal(0)
            for risk_level_name, risk_level_config in risk_config.risk_scores.items():
                if (
                    risk_level_config.min_value
                    <= transaction.score
                    <= risk_level_config.max_value
                ):
                    for address, payout_ratio in risk_level_config.addresses.items():
                        external_drain_list.append(
                            [
                                address,
                                payout_ratio,
                            ]
                        )

                    incomplete_payouts = []

                    for payout in external_drain_list:
                        address, payout_ratio = payout
                        if address not in addresses_done:
                            incomplete_payouts.append(address)

                    if not incomplete_payouts:
                        logger.debug(
                            f"Payout has already been done for {short_txid(tx_id)}"
                        )
                        return False

                    for i in range(len(external_drain_list) - 1):
                        payout_ratio = external_drain_list[i][1]
                        amount_to_address = transaction.amount * payout_ratio
                        external_amounts = external_amounts + amount_to_address
                        external_drain_list[i][1] = amount_to_address
                        external_drain_list[i].append(amount_to_address)

                    # send the rest to the last addresss in list
                    the_rest = transaction.amount - external_amounts
                    external_drain_list[-1][1] = the_rest
                    external_drain_list[-1].append(the_rest)

                    new_payout_list = []
                    for payout in external_drain_list:
                        if payout[0] not in addresses_done:
                            new_payout_list.append(payout)

                    logger.info(
                        f"{short_txid(transaction.tx_id)} "
                        f"AML score: {transaction.score} matches '{risk_level_name}' payout rule"
                    )
                    logger.info(f"{short_txid(transaction.tx_id)} payout list:")
                    for payout in external_drain_list:
                        logger.info(f"{payout[1]} {symbol} -> {payout[0]}")

                    return new_payout_list

        elif transaction.ttype == "aml" and transaction.status == "pending":
            return False

        elif transaction.ttype == "aml" and transaction.status == "rechecking":
            return False

        elif transaction.ttype == "regular":
            return False

        else:
            logger.warning(
                f"Unknown status {transaction.status} for transaction {short_txid(transaction.tx_id)}"
            )
            return False

    elif "regular" == payout_type:
        if transaction.ttype == "regular" and transaction.status == "drained":
            return False
        external_amounts = Decimal(0)
        regular_split_config = config.EXTERNAL_DRAIN_CONFIG.regular_split.cryptos[
            symbol
        ]
        for address, payout_ratio in regular_split_config.addresses.items():
            external_drain_list.append([address, payout_ratio])

        incomplete_payouts = []
        for payout in external_drain_list:
            address, payout_ratio = payout
            if address not in addresses_done:
                incomplete_payouts.append(address)

        if not incomplete_payouts:
            return False

        else:
            for i in range(0, len(external_drain_list) - 1):
                payout_ratio = external_drain_list[i][1]
                amount_to_address = transaction.amount * payout_ratio
                external_amounts = external_amounts + amount_to_address
                external_drain_list[i][1] = amount_to_address
                external_drain_list[i].append(amount_to_address)

            # send the rest to the last addresss in list
            the_rest = transaction.amount - external_amounts
            external_drain_list[-1][1] = the_rest
            external_drain_list[-1].append(the_rest)

            new_payout_list = []
            for payout in external_drain_list:
                if payout[0] not in addresses_done:
                    new_payout_list.append(payout)

            logger.info(f"{short_txid(transaction.tx_id)} payout list:")
            for payout in external_drain_list:
                logger.info(f"{payout[1]} {symbol} -> {payout[0]}")

            return new_payout_list

    else:
        logger.error(
            f"Can't build payout list: "
            f"check that {symbol} is configured in EXTERNAL_DRAIN_CONFIG"
        )
        return False
