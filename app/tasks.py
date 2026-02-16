import collections
import concurrent
from contextlib import closing
import datetime
import decimal
from functools import cache, lru_cache
import json
import math
import sqlite3
import time
from decimal import Decimal
from typing import Dict, List

from celery import Celery
from celery.schedules import crontab
from pydantic import TypeAdapter
from tronpy.keys import PrivateKey
from tronpy.tron import current_timestamp
from tronpy.abi import trx_abi
import tronpy.exceptions
import requests
from sqlmodel import Session, select

from app.schemas import KeyType

from . import celery
from .config import config
from .db import query_db, query_db2
from .wallet import Wallet
from .utils import (
    est_vote_tx_bw_cons,
    get_energy_delegator,
    get_key,
    has_free_bw,
    skip_if_running,
)
from .connection_manager import ConnectionManager
from .logging import logger
from .wallet_encryption import wallet_encryption


@celery.task()
def prepare_payout(dest, amount, symbol):
    if (balance := Wallet(symbol).balance) < amount:
        raise Exception(
            f"Wallet balance is less than payout amount: {balance} < {amount}"
        )
    steps = []
    steps.append(
        {
            "dst": dest,
            "amount": decimal.Decimal(amount),
        }
    )
    return steps


@celery.task()
def prepare_multipayout(payout_list, symbol):
    logger.info(
        f"Preparing payout for {sum([t['amount'] for t in payout_list])} "
        f"{symbol} to {len(payout_list)} destinations."
    )
    steps = []
    for payout in payout_list:
        steps.append(
            {
                "dst": payout["dest"],
                "amount": decimal.Decimal(payout["amount"]),
            }
        )
    return steps


@celery.task()
def payout(steps, symbol):
    wallet = Wallet(symbol)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.CONCURRENT_MAX_WORKERS
    ) as executor:
        payout_results = list(
            executor.map(lambda x: wallet.transfer(x["dst"], x["amount"]), steps)
        )
    post_payout_results.delay(payout_results, symbol)
    return payout_results


@celery.task()
def transfer_trc20_from(onetime_acc, symbol):
    """
    Transfers TRC20 from onetime to main account
    """

    tron_client = ConnectionManager.client()

    contract_address = config.get_contract_address(symbol)
    contract = tron_client.get_contract(contract_address)
    precision = contract.functions.decimals()

    main_priv_key, main_publ_key = get_key(KeyType.fee_deposit)

    if onetime_acc == main_publ_key:
        logger.warning(
            "Transfer from main account is not allowed. Terminating transfer."
        )
        return False

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    onetime_priv_key, onetime_publ_key = get_key(KeyType.onetime, pub=onetime_acc)

    token_balance = contract.functions.balanceOf(onetime_publ_key)

    tx_trx_res = None

    def calc_sun_for_energy_delegation(energy, res):
        trx: int = math.ceil(
            (res["TotalEnergyWeight"] * energy) / res["TotalEnergyLimit"]
        )
        trx *= config.ENERGY_DELEGATION_MODE_ENERGY_DELEGATION_FACTOR
        return int(trx * 1_000_000)

    def delegate_energy(sun_to_delegate):
        logger.info("Check if energy delegator account can delegate energy")
        result = tron_client.provider.make_request(
            "wallet/getcandelegatedmaxsize",
            {"owner_address": energy_delegator_pub, "type": 1, "visible": True},
        )
        if "max_size" not in result:
            logger.warning(
                "Energy delegator has no delegatable energy. Terminating transfer."
            )
            return False

        else:
            delegetable_sun = result["max_size"]

            logger.info(f"{delegetable_sun=} {sun_to_delegate=}")

            if delegetable_sun < sun_to_delegate:
                logger.warning(
                    "Energy delegator has not enough energy. Terminating transfer."
                )
                return False
            else:
                logger.info("Energy delegator has enough energy")

                logger.info("Delegating energy to onetime account")

                unsigned_tx = tron_client.trx.delegate_resource(
                    owner=energy_delegator_pub,
                    receiver=onetime_publ_key,
                    balance=sun_to_delegate,
                    resource="ENERGY",
                ).build()
                signed_tx = unsigned_tx.sign(energy_delegator_priv)
                logger.info(f"TX json size: {len(json.dumps(signed_tx._raw_data))}")

                delegate_tx_info = signed_tx.broadcast().wait()

                logger.info(
                    f"Delegated {energy_needed} energy to onetime account {onetime_publ_key} with TXID: {unsigned_tx.txid}"
                )
                logger.info(delegate_tx_info)

                logger.info(
                    "Recheck resources of the onetime address after energy delegation"
                )
                onetime_address_resources = tron_client.get_account_resource(
                    onetime_publ_key
                )
                onetime_energy_available = onetime_address_resources.get(
                    "EnergyLimit", 0
                )
                logger.info(
                    f"{onetime_publ_key=} {onetime_energy_available=} {energy_needed=}"
                )
                if onetime_energy_available < energy_needed:
                    logger.warning(
                        "Onetime account has not enough energy after delegation. Terminating transfer."
                    )
                    return False
                else:
                    logger.info("Energy successfuly delegated")
                    return True

    logger.info(f"Check ONETIME={onetime_publ_key} {symbol} balance")
    min_threshold = config.get_min_transfer_threshold(symbol)
    balance = Decimal(token_balance) / 10**precision
    if balance <= min_threshold:
        logger.warning(
            f"Treshold not reached for {onetime_publ_key}. Has: {balance} {symbol} need: {min_threshold} {symbol}. Terminating transfer."
        )
        return
    else:
        logger.info(
            f"Balance OK: {balance} {symbol}. Threshold: {min_threshold} {symbol}"
        )

    if config.ENERGY_DELEGATION_MODE:
        logger.info(
            f"Initiating TRC20 tokens transfer from ONETIME={onetime_publ_key} to MAIN={main_publ_key} in ENERGY DELEGATION MODE"
        )

        need_bw = (
            config.BANDWIDTH_PER_DELEGE_CALL
            + config.BANDWIDTH_PER_UNDELEGATE_CALL
            + config.BANDWIDTH_PER_TRX_TRANSFER
        )
        logger.info(f"Estimated bandwidth requirement: {need_bw}")

        logger.info("Check energy delegator bandwidth")
        if has_free_bw(energy_delegator_pub, need_bw):
            logger.info("Using free bandwidth")
        else:
            logger.info("Not enough free bandwidth")
            if config.ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_FOR_BANDWITH:
                logger.info("Burning TRX for bandwidth")
            else:
                logger.warning(
                    "Burning TRX for bandwidth is not allowed. Terminating transfer."
                )
                return

        try:
            onetime_address_resources = tron_client.get_account_resource(
                onetime_publ_key
            )
            logger.info(
                f"Onetime {onetime_publ_key} is already on chain, skipping activation. Resource details {onetime_address_resources=}"
            )
        except tronpy.exceptions.AddressNotFound:
            TRX_FOR_ACTIVATION = "1.1"
            logger.info(
                f"Check if main account has {TRX_FOR_ACTIVATION} TRX for activation"
            )
            main_trx_balance = tron_client.get_account_balance(main_publ_key)
            logger.info(f"Main account balance: {main_trx_balance} TRX")
            if main_trx_balance < Decimal(TRX_FOR_ACTIVATION):
                logger.warning(
                    f"Not enough TRX to activate {onetime_publ_key}. Terminating transfer."
                )
                return
            else:
                logger.info("Main account TRX balance OK.")

            logger.info("Check main account free bandwidth")
            if has_free_bw(
                main_publ_key, config.BANDWIDTH_PER_TRX_TRANSFER, use_only_staked=True
            ):
                logger.info("Using main account free bandwidth")
            else:
                logger.info("Main account has not enough free bandwidth")
                if config.ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_FOR_BANDWITH:
                    logger.info("Burning TRX for bandwidth")
                else:
                    logger.warning(
                        "Burning TRX for bandwidth is not allowed. Terminating transfer."
                    )
                    return

            logger.info(f"Activating {onetime_publ_key} by sending 0.1 TRX")
            tx_trx = tron_client.trx.transfer(
                main_publ_key,
                onetime_publ_key,
                int(0.1 * 1_000_000),
            )
            tx_trx._raw_data["expiration"] = current_timestamp() + 60_000
            tx_trx = tx_trx.build()
            tx_trx = tx_trx.sign(main_priv_key)
            tx_trx_res = tx_trx.broadcast().wait()
            logger.info(f"0.1 TRX sent. Details: {tx_trx_res}")
            onetime_address_resources = tron_client.get_account_resource(
                onetime_publ_key
            )
            try:
                onetime_address_resources = tron_client.get_account_resource(
                    onetime_publ_key
                )
            except tronpy.exceptions.AddressNotFound:
                logger.warning(
                    "Onetime acount still not on chain after activation. Terminating transfer."
                )
                return

        logger.info("Estimate the amount of energy needed to make transfer")
        energy_needed = tron_client.get_estimated_energy(
            onetime_publ_key,
            contract_address,
            "transfer(address,uint256)",
            trx_abi.encode_single("(address,uint256)", (main_publ_key, 42)).hex(),
        )
        logger.info(f"Estimated amount of energy for transfer is: {energy_needed}")

        logger.info("Check the energy of onetime address")

        onetime_energy_available = onetime_address_resources.get("EnergyLimit", 0)
        if onetime_energy_available >= energy_needed:
            logger.info(
                f"Onetime account {onetime_publ_key} has {onetime_energy_available} "
                f"of {energy_needed} energy. Skipping delegation."
            )

        else:
            logger.info(
                f"Onetime account {onetime_publ_key} has {onetime_energy_available} "
                f"of {energy_needed} energy"
            )

            logger.info("Check if energy was alread delegated")

            onetime_delegated_resources = (
                tron_client.get_delegated_resource_account_index_v2(onetime_publ_key)
            )

            if "fromAccounts" in onetime_delegated_resources:
                logger.info(
                    f"Found delegated energy on onetime account. Details {onetime_delegated_resources=}"
                )

                if onetime_energy_available < energy_needed:
                    logger.warning(
                        "Onetime account has not enough energy after previous delegation."
                    )

                    if config.ENERGY_DELEGATION_MODE_ALLOW_ADDITIONAL_ENERGY_DELEGATION:
                        logger.info(
                            "Additional energy delegation is allowed. Calculating the difference."
                        )
                        energy_diff = energy_needed - onetime_energy_available

                        if energy_diff <= 0:
                            logger.warning(
                                f"Energy diff = {energy_diff}. Terminating transfer."
                            )

                        sun_needed = calc_sun_for_energy_delegation(
                            energy_diff, onetime_address_resources
                        )
                        logger.info(
                            f"Energy diff is {energy_diff}. TRX to delegate: {sun_needed / 1_000_000}"
                        )
                    else:
                        logger.warning("Terminating transfer.")
                        return
            else:
                logger.info("No delagated energy found")
                sun_needed = calc_sun_for_energy_delegation(
                    energy_needed, onetime_address_resources
                )

            logger.info(
                f"Delegating {sun_needed / 1_000_000} TRX to {onetime_publ_key}"
            )
            if not delegate_energy(sun_needed):
                return

            # Check available bandwidth before transfer trc20 tokens
            # from one_time to fee_deposit account
            if not has_free_bw(
                onetime_publ_key, config.BANDWIDTH_PER_TRC20_TRANSFER_CALL
            ):
                logger.warning(
                    "One-time account has no bandwidth. Terminating transfer."
                )
                return
    else:
        logger.info(
            "Transferring TRC20 tokens from onetime to main in TRX burning mode"
        )

        logger.info(
            f"Transfer to main acc started for {onetime_publ_key}. Balance: "
            f"{balance} {symbol}. Threshold is {min_threshold} {symbol}"
        )

        main_acc_balance = tron_client.get_account_balance(main_publ_key)

        if main_acc_balance < config.get_internal_trc20_tx_fee():
            logger.warning(
                f"Main account hasn't enough currency: balance: {main_acc_balance} need: {config.get_internal_trc20_tx_fee()}.  Terminating transfer."
            )
            return

        tx_trx = tron_client.trx.transfer(
            main_publ_key,
            onetime_publ_key,
            int(config.get_internal_trc20_tx_fee() * 1_000_000),
        )
        tx_trx._raw_data["expiration"] = current_timestamp() + 60_000
        tx_trx = tx_trx.build()
        tx_trx = tx_trx.sign(main_priv_key)
        tx_trx_res = tx_trx.broadcast().wait()
        logger.info(
            f"Fee sent to {onetime_publ_key} with TXID {tx_trx.txid}. Details: {tx_trx_res}"
        )

    #
    # Same flow for both modes
    #

    tx_token = contract.functions.transfer(main_publ_key, int(token_balance))
    tx_token = tx_token.with_owner(onetime_publ_key)
    tx_token = tx_token.fee_limit(int(config.TX_FEE_LIMIT * 1_000_000))
    tx_token._raw_data["expiration"] = current_timestamp() + 60_000
    tx_token = tx_token.build()
    tx_token = tx_token.sign(onetime_priv_key)
    tx_token_res = tx_token.broadcast().wait()
    logger.info(
        f"{token_balance / 10**precision} {symbol} sent to {main_publ_key} with {tx_token.txid}. Details: {tx_token_res}"
    )

    if config.ENERGY_DELEGATION_MODE:
        if config.DEVMODE_CELERY_NODELAY:
            undelegate_energy(onetime_publ_key)
        else:
            undelegate_energy.delay(onetime_publ_key)

    return {"tx_trx_res": tx_trx_res, "tx_token": tx_token_res}


@celery.task()
def undelegate_energy(receiver):
    logger.info(f"Undelegating energy from onetime account {receiver}")

    tron_client = ConnectionManager.client()

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()

    result = tron_client.get_delegated_resource_v2(
        fromAddr=energy_delegator_pub, toAddr=receiver
    )
    if "delegatedResource" not in result:
        logger.info(
            f"Onetime account {receiver} has no any resources delegated. Skipping undelegation."
        )
        return
    frozen_balance_for_energy = 0
    for resource in result["delegatedResource"]:
        if (
            "frozen_balance_for_energy" in resource
            and resource["from"] == energy_delegator_pub
        ):
            frozen_balance_for_energy += resource["frozen_balance_for_energy"]
    if not frozen_balance_for_energy:
        logger.info(
            f"Onetime account {receiver} has no energy delegated. "
            f"Skipping undelegation. Resource details: {result}"
        )
        return

    logger.info(
        f"Undelegating {frozen_balance_for_energy / 1_000_000} TRX from {receiver}"
    )

    unsigned_tx = tron_client.trx.undelegate_resource(
        owner=energy_delegator_pub,
        receiver=receiver,
        balance=frozen_balance_for_energy,
        resource="ENERGY",
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    undelegate_tx_info = signed_tx.broadcast().wait()

    logger.info(
        f"Undelegated {frozen_balance_for_energy / 1_000_000} TRX from {receiver} with TXID: {unsigned_tx.txid}"
    )
    logger.debug(undelegate_tx_info)


@celery.task()
def transfer_trx_from(onetime_publ_key):
    """
    Transfers TRX from onetime to main account
    """
    logger.info(f"Starting TRX transfer from onetime account {onetime_publ_key}")
    main_publ_key = query_db2(
        'select * from keys where type = "fee_deposit" ', one=True
    )["public"]

    if main_publ_key == onetime_publ_key:
        logger.warning("Skipping TRX transfer from main account.")
        return {"status": "error", "error": "Skipping TRX transfer from main account."}

    bw = Wallet().bandwidth_of(onetime_publ_key)
    if bw < config.BANDWIDTH_PER_TRX_TRANSFER:
        logger.info(
            f"{onetime_publ_key} has not enough bandwidth "
            f"for a free transfer ({bw}/{config.BANDWIDTH_PER_TRX_TRANSFER})"
        )
        return

    tron_client = ConnectionManager.client()
    onetime_priv_key = PrivateKey(
        bytes.fromhex(
            wallet_encryption.decrypt(
                query_db2(
                    'select * from keys where type = "onetime" and public = ?',
                    (onetime_publ_key,),
                    one=True,
                )["private"]
            )
        )
    )

    onetime_acc_balance = tron_client.get_account_balance(onetime_publ_key)
    if onetime_acc_balance == 0:
        return {"status": "error", "error": "skipping 0 TRX account"}

    tx_trx = tron_client.trx.transfer(
        onetime_publ_key, main_publ_key, int(onetime_acc_balance * 1_000_000)
    )
    tx_trx._raw_data["expiration"] = current_timestamp() + 60_000
    tx_trx = tx_trx.build()
    tx_trx = tx_trx.sign(onetime_priv_key)
    tx_trx_res = tx_trx.broadcast().wait()
    logger.info(
        f"{onetime_acc_balance} TRX sent to main account ({main_publ_key}) with TXID {tx_trx.txid}. Details: {tx_trx_res}"
    )
    return {"tx_trx_res": tx_trx_res}


@celery.task()
def post_payout_results(data, symbol):
    while True:
        try:
            return requests.post(
                f"http://{config.SHKEEPER_HOST}/api/v1/payoutnotify/{symbol}",
                headers={"X-Shkeeper-Backend-Key": config.SHKEEPER_BACKEND_KEY},
                json=data,
            )
        except Exception as e:
            logger.warning(f"Shkeeper payout notification failed: {e}")
            time.sleep(10)


def is_task_running(task_instance, name: str, args: List = None, kwargs: Dict = None):
    workers = task_instance.app.control.inspect().active()
    for worker, tasks in workers.items():
        for task in tasks:
            # check if task name matches
            if task["name"] != name:
                continue
            # check if args is subset of task args
            if args and not (set(args) <= set(task["args"])):
                continue
            # check if kwargs is subset of task kwargs
            if kwargs and not (kwargs.items() <= task["kwargs"].items()):
                continue
            return True
    return False


@celery.task(bind=True)
@skip_if_running
def scan_accounts(self, *args, **kwargs):
    """
    Scans onetime accounts balances (trc20, trx),
    saves it to database and transfers to main account.
    """

    from .db import engine
    from .models import Balance

    with Session(engine) as session:
        stats = {
            "balances": collections.defaultdict(Decimal),
            "exception_num": 0,
        }

        accounts = [
            row["public"]
            for row in query_db('SELECT public FROM keys WHERE type = "onetime"')
        ]

        balances_to_collect = {"trx": [], "trc20": []}

        for index, account in enumerate(accounts, start=1):
            try:
                #
                # TRC20
                #

                for symbol in [token.symbol for token in config.get_tokens()]:
                    contract = ConnectionManager.client().get_contract(
                        config.get_contract_address(symbol)
                    )

                    while ret := 0 < config.CONCURRENT_MAX_RETRIES:
                        try:
                            trc20_balance = Decimal(
                                contract.functions.balanceOf(account)
                            ) / (10 ** config.get_decimal(symbol))
                            break
                        except tronpy.exceptions.UnknownError as e:
                            logger.debug(
                                f"{account} {symbol} trc20 balance fetch error: {e}"
                            )
                            ret += 1
                    else:
                        raise Exception(
                            f"CONCURRENT_MAX_RETRIES reached while getting trc20 balance of {account}"
                        )

                    stats["balances"][symbol] += trc20_balance

                    if config.SAVE_BALANCES_TO_DB:
                        acc_balance = session.exec(
                            select(Balance).where(
                                Balance.account == account, Balance.symbol == symbol
                            )
                        ).first()
                        if acc_balance:
                            acc_balance.balance = trc20_balance

                        else:
                            acc_balance = Balance()
                            acc_balance.account = account
                            acc_balance.symbol = symbol
                            acc_balance.balance = trc20_balance
                        session.add(acc_balance)
                        session.commit()

                    if trc20_balance > 0:
                        balances_to_collect["trc20"].append(
                            [account, symbol, trc20_balance]
                        )

                #
                # TRX
                #

                while ret := 0 < config.CONCURRENT_MAX_RETRIES:
                    try:
                        trx_balance = ConnectionManager.client().get_account_balance(
                            account
                        )
                        break
                    except tronpy.exceptions.AddressNotFound:
                        trx_balance = Decimal(0)
                        break
                    except tronpy.exceptions.UnknownError as e:
                        logger.debug(f"{account} TRX balance fetch error: {e}")
                        ret += 1
                else:
                    raise Exception(
                        f"CONCURRENT_MAX_RETRIES reached while getting TRX balance of {account}"
                    )

                stats["balances"]["TRX"] += trx_balance

                if config.SAVE_BALANCES_TO_DB:
                    acc_balance = session.exec(
                        select(Balance).where(
                            Balance.account == account, Balance.symbol == "TRX"
                        )
                    ).first()
                    if acc_balance:
                        acc_balance.balance = trx_balance

                    else:
                        acc_balance = Balance()
                        acc_balance.account = account
                        acc_balance.symbol = "TRX"
                        acc_balance.balance = trx_balance
                    session.add(acc_balance)
                    session.commit()

                if trx_balance > 0:
                    balances_to_collect["trx"].append([account, trx_balance])

                logger.debug(
                    f"Scanned {index} of {len(accounts)} accounts, found: "
                    + ", ".join([f"{v} {k}" for k, v in stats["balances"].items()])
                )

            except Exception as e:
                logger.exception(f"{account} scan error: {e}")
                stats["exception_num"] += 1

        # Sort trc20 balances by balance in descending order
        balances_to_collect["trc20"].sort(key=lambda x: x[2], reverse=True)
        logger.info("TRC20 queue length: %d" % len(balances_to_collect["trc20"]))
        # Log histogram of TRC20 balances
        bins = [5, 50, 100, 300, 500, 1000, 2000]
        histogram = collections.Counter()
        for _, _, balance in balances_to_collect["trc20"]:
            for b in bins:
                if balance < b:
                    histogram[f"<{b}"] += 1
                    break
            else:
                histogram[">=2000"] += 1
        logger.info(
            "TRC20 balances histogram: "
            + ", ".join([f"{k}: {v}" for k, v in histogram.items()])
        )
        for account, symbol, trc20_balance in balances_to_collect["trc20"]:
            if not is_task_running(
                self,
                "app.tasks.transfer_trc20_from",
                args=[account, symbol],
            ):
                transfer_trc20_from(account, symbol)

        # Sort trx balances by balance in descending order
        balances_to_collect["trx"].sort(key=lambda x: x[1], reverse=True)
        # logger.info(balances_to_collect["trx"])
        for account, trx_balance in balances_to_collect["trx"]:
            if not is_task_running(
                self, "app.tasks.transfer_trc20_from", args=[account]
            ):
                # We don't need to check if account has a free bandwidth because tx will raise tronpy.exceptions.ValidationError
                # if there is not enough TRX to burn for bandwidth. We are sending the entire TRX balance,
                # so there will be no TRX to burn for sure.
                transfer_trx_from(account)

    return stats


@celery.task(bind=True)
@skip_if_running
def vote_for_sr(self, *args, **kwargs):
    logger.info("Checking voting config")
    if not config.SR_VOTES:
        logger.warning("Voting enabled but no config given. Terminating voting task.")
        return
    logger.info(f"Voting config is OK: {config.SR_VOTES}")
    tron_client = ConnectionManager.client()

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()

    logger.info(f"Checking current votes for {energy_delegator_pub}")
    acc_info = tron_client.get_account(energy_delegator_pub)

    if "votes" in acc_info:
        from .schemas import SrVote

        ta = TypeAdapter(List[SrVote])
        votes = ta.validate_python(acc_info["votes"])

        if config.SR_VOTES == votes:
            logger.info("Already voted according to config. Terminating voting task.")
            return
        else:
            logger.info("Voting config doesn't match previous voting.")
            logger.info("Revoting.")
    else:
        logger.info("Account hasn't voted yet.")
        logger.info("Voting.")

    logger.info(f"Check {energy_delegator_pub} bandwidth")
    need_bw = est_vote_tx_bw_cons(len(config.SR_VOTES))
    logger.info(
        f"Estimated bandwith requirement to vote "
        f"for {len(config.SR_VOTES)} SRs is: {need_bw}"
    )
    if has_free_bw(energy_delegator_pub, need_bw):
        logger.info("Using free bandwidth")
    else:
        logger.info("Available free bandwith points is not enough to vote")
        if config.SR_VOTING_ALLOW_BURN_TRX:
            logger.info("Voting will burn TRX for bandwidth points")
        else:
            logger.warning(
                "Burning TRX for bandwidth points is not allowed. Terminating voting."
            )
            return

    unsigned_tx = tron_client.trx.vote_witness(
        energy_delegator_pub,
        *[(v.vote_address, v.vote_count) for v in config.SR_VOTES],
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    tx_info = signed_tx.broadcast().wait()

    logger.info(f"Voting complete. TX details: {tx_info}")


@celery.task(bind=True)
@skip_if_running
def claim_reward(self, *args, **kwargs):
    # TODO: implement automatic reward claims
    # logger.info("Checking voting config")
    # if not config.SR_VOTES:
    #     logger.warning("Voting enabled but no config given. Terminating voting task.")
    #     return
    # logger.info(f"Voting config is OK: {config.SR_VOTES}")
    # tron_client = ConnectionManager.client()
    # main_acc_keys = query_db2(
    #     'select * from keys where type = "fee_deposit" ', one=True
    # )
    # main_priv_key = PrivateKey(
    #     bytes.fromhex(wallet_encryption.decrypt(main_acc_keys["private"]))
    # )
    # main_publ_key = main_acc_keys["public"]
    # logger.info(f"Checking current votes for {main_publ_key}")
    # acc_info = tron_client.get_account(main_publ_key)
    # # "allowance": 16678,
    # # "latest_withdraw_time": 1752679503000,
    # # once every 24 h
    pass


@celery.on_after_configure.connect
def setup_periodic_tasks(sender: Celery, **kwargs):
    pass
