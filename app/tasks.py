import collections
import concurrent
from contextlib import closing
import datetime
import decimal
from functools import cache, lru_cache
import json
import logging
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
from app.schemas import KeyType

from . import celery
from .config import config
from .wallet import Wallet
from .repositories import AllStoresKeyReader, BalanceRepository, KeyRepository
from .utils import (
    est_vote_tx_bw_cons,
    estimate_energy_wait,
    get_energy_delegator,
    get_key,
    has_free_bw,
    skip_if_running,
)
from .connection_manager import ConnectionManager
from .logging import logger
from .wallet_encryption import wallet_encryption


def _tenant_logger(store_id: int):
    class TenantLoggerAdapter(logging.LoggerAdapter):
        def process(self, msg, kwargs):
            store_id = self.extra.get("store_id") if self.extra else None
            return f"[store_id={store_id}] {msg}", kwargs

    return TenantLoggerAdapter(logger, {"store_id": store_id})


@celery.task()
def prepare_payout(dest, amount, symbol, store_id: int = 1):
    if (balance := Wallet(symbol, store_id=store_id).balance) < amount:
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
def prepare_multipayout(payout_list, symbol, store_id: int = 1):
    logger = _tenant_logger(store_id)
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
def payout(steps, symbol, store_id: int = 1):
    wallet = Wallet(symbol, store_id=store_id)
    with concurrent.futures.ThreadPoolExecutor(
        max_workers=config.CONCURRENT_MAX_WORKERS
    ) as executor:
        payout_results = list(
            executor.map(lambda x: wallet.transfer(x["dst"], x["amount"]), steps)
        )
    post_payout_results.delay(payout_results, symbol, store_id)
    return payout_results


@celery.task()
def transfer_trc20_from(onetime_acc, symbol, store_id: int = 1):
    """
    Transfers TRC20 from onetime to main account
    """
    logger = _tenant_logger(store_id)

    tron_client = ConnectionManager.client()

    contract_address = config.get_contract_address(symbol)
    contract = tron_client.get_contract(contract_address)
    precision = contract.functions.decimals()

    main_priv_key, main_publ_key = get_key(KeyType.fee_deposit, store_id=store_id)

    if onetime_acc == main_publ_key:
        logger.warning(
            "Transfer from main account is not allowed. Terminating transfer."
        )
        return False

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator(
        store_id=store_id
    )
    onetime_priv_key, onetime_publ_key = get_key(
        KeyType.onetime, pub=onetime_acc, store_id=store_id
    )

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

        if config.ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE:
            logger.info(
                "Transfer energy estimate overriden to: "
                f"{config.ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE}"
            )
            energy_needed = (
                config.ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE
            )
        else:
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
            undelegate_energy(onetime_publ_key, store_id=store_id)
        else:
            undelegate_energy.delay(onetime_publ_key, store_id=store_id)

    return {"tx_trx_res": tx_trx_res, "tx_token": tx_token_res}


@celery.task()
def undelegate_energy(receiver, store_id: int = 1):
    logger = _tenant_logger(store_id)
    logger.info(f"Undelegating energy from onetime account {receiver}")

    tron_client = ConnectionManager.client()

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator(
        store_id=store_id
    )

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
def transfer_trx_from(onetime_publ_key, store_id: int = 1):
    """
    Transfers TRX from onetime to main account
    """
    logger = _tenant_logger(store_id)
    logger.info(f"Starting TRX transfer from onetime account {onetime_publ_key}")
    key_repository = KeyRepository(store_id=store_id)
    main_key = key_repository.get_fee_deposit_key()
    if not main_key:
        raise RuntimeError("fee_deposit key unavailable")
    main_publ_key = main_key["public"]

    if main_publ_key == onetime_publ_key:
        logger.warning("Skipping TRX transfer from main account.")
        return {"status": "error", "error": "Skipping TRX transfer from main account."}

    bw = Wallet(store_id=store_id).bandwidth_of(onetime_publ_key)
    if bw < config.BANDWIDTH_PER_TRX_TRANSFER:
        logger.info(
            f"{onetime_publ_key} has not enough bandwidth "
            f"for a free transfer ({bw}/{config.BANDWIDTH_PER_TRX_TRANSFER})"
        )
        return

    tron_client = ConnectionManager.client()
    onetime_key = key_repository.get_by_public_and_type(
        onetime_publ_key, "onetime"
    )
    if not onetime_key:
        raise RuntimeError(f"onetime key unavailable: {onetime_publ_key}")
    onetime_priv_key = PrivateKey(
        bytes.fromhex(wallet_encryption.decrypt(onetime_key["private"]))
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
def post_payout_results(data, symbol, store_id: int = 1):
    logger = _tenant_logger(store_id)
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
def balance_collector(self, *args, **kwargs):
    """
    Scans onetime accounts balances (trc20, trx) and saves them to the database.
    Sweeping is handled separately by funds_sweeper.
    """
    if is_task_running(self, "app.tasks.funds_sweeper"):
        logger.info("balance_collector: funds_sweeper is running, skipping this run")
        return "funds_sweeper is running, skipping"

    task_start = time.monotonic()
    balance_repository = BalanceRepository()
    _progress_interval = config.SCAN_ACCOUNTS_PROGRESS_LOG_INTERVAL

    stats = {
        "balances": collections.defaultdict(Decimal),
        "exception_num": 0,
    }
    found_positive_balance = False

    account_rows = AllStoresKeyReader().list_onetime_accounts()
    accounts = [row["public"] for row in account_rows]
    account_store_ids = {row["public"]: int(row["store_id"]) for row in account_rows}

    total = len(accounts)
    logger.info(f"balance_collector: scanning {total} onetime accounts")
    collection_loop_start = time.monotonic()
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
                            f"[store_id={account_store_ids[account]}] "
                            f"{account} {symbol} trc20 balance fetch error: {e}"
                        )
                        ret += 1
                else:
                    raise Exception(
                        f"CONCURRENT_MAX_RETRIES reached while getting trc20 balance of {account}"
                    )

                stats["balances"][symbol] += trc20_balance

                if config.SAVE_BALANCES_TO_DB:
                    balance_repository.upsert(account, symbol, trc20_balance)

                if trc20_balance > 0:
                    found_positive_balance = True

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
                    logger.debug(
                        f"[store_id={account_store_ids[account]}] "
                        f"{account} TRX balance fetch error: {e}"
                    )
                    ret += 1
            else:
                raise Exception(
                    f"CONCURRENT_MAX_RETRIES reached while getting TRX balance of {account}"
                )

            stats["balances"]["TRX"] += trx_balance

            if config.SAVE_BALANCES_TO_DB:
                balance_repository.upsert(account, "TRX", trx_balance)

            if trx_balance > 0:
                found_positive_balance = True

            logger.debug(
                f"Scanned {index} of {len(accounts)} accounts, found: "
                + ", ".join([f"{v} {k}" for k, v in stats["balances"].items()])
            )
            if (
                total > 0
                and (index * 100 // total) // _progress_interval
                > ((index - 1) * 100 // total) // _progress_interval
            ):
                _now = time.monotonic()
                logger.info(
                    f"balance_collector: {index * 100 // total}% ({index}/{total} accounts)"
                    f" | loop {_now - collection_loop_start:.1f}s | task {_now - task_start:.1f}s"
                )

        except Exception as e:
            logger.exception(
                f"[store_id={account_store_ids[account]}] {account} scan error: {e}"
            )
            stats["exception_num"] += 1

    if found_positive_balance:
        logger.info("balance_collector: positive balances found, triggering funds_sweeper")
        funds_sweeper.delay()
    else:
        logger.info("balance_collector: no positive balances found, not triggering funds_sweeper")

    return stats


@celery.task(bind=True)
@skip_if_running
def funds_sweeper(self, *args, **kwargs):
    """
    Sweeps the onetime account with the largest TRC20 balance, then unconditionally
    sweeps every onetime account holding only TRX. Chains itself while TRC20 balances
    remain, or triggers balance_collector once none are left.
    """
    if is_task_running(self, "app.tasks.balance_collector"):
        logger.info("funds_sweeper: balance_collector is running, skipping this run")
        return "balance_collector is running, skipping"

    balance_repository = BalanceRepository()

    initial_trc20_row = balance_repository.get_top_trc20_balance()

    skip_trc20_this_cycle = False

    if initial_trc20_row:
        account = initial_trc20_row["account"]
        symbol = initial_trc20_row["symbol"]
        balance = initial_trc20_row["balance"]
        store_id = initial_trc20_row["store_id"]
        logger.info(
            f"[store_id={store_id}] funds_sweeper: top TRC20 candidate "
            f"{account} {balance} {symbol}"
        )

        if config.ENERGY_DELEGATION_MODE:
            tron_client = ConnectionManager.client()
            _, energy_delegator_pub = get_energy_delegator(store_id=store_id)
            _, main_publ_key = get_key(KeyType.fee_deposit, store_id=store_id)

            if config.ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE:
                required_energy = (
                    config.ENERGY_DELEGATION_MODE_TRC20_TRANSFER_ENERGY_ESTIMATE_OVERRIDE
                )
            else:
                required_energy = tron_client.get_estimated_energy(
                    account,
                    config.get_contract_address(symbol),
                    "transfer(address,uint256)",
                    trx_abi.encode_single(
                        "(address,uint256)", (main_publ_key, 42)
                    ).hex(),
                )

            try:
                wait_seconds = estimate_energy_wait(
                    tron_client, energy_delegator_pub, required_energy
                )
            except RuntimeError as e:
                logger.warning(
                    f"[store_id={store_id}] energy delegator {energy_delegator_pub} "
                    f"recovery rate unavailable, skipping TRC20 sweep this cycle: {e}"
                )
                skip_trc20_this_cycle = True
                wait_seconds = 0

            if not skip_trc20_this_cycle and wait_seconds > config.ENERGY_WAIT_MAX_SECONDS:
                logger.warning(
                    f"[store_id={store_id}] energy delegator {energy_delegator_pub} "
                    f"needs {wait_seconds:.0f}s to recover {required_energy} energy, "
                    f"exceeds ENERGY_WAIT_MAX_SECONDS={config.ENERGY_WAIT_MAX_SECONDS}. "
                    "Skipping TRC20 sweep this cycle."
                )
                skip_trc20_this_cycle = True
            elif not skip_trc20_this_cycle and wait_seconds > 0:
                logger.info(
                    f"[store_id={store_id}] waiting {wait_seconds:.0f}s for energy "
                    f"delegator {energy_delegator_pub} to recover {required_energy} energy"
                )
                time.sleep(wait_seconds)

    else:
        logger.info("funds_sweeper: no TRC20 candidates found")

    # Re-select the top balance after any energy wait, since it may no longer be the top one
    trc20_row = (
        balance_repository.get_top_trc20_balance()
        if initial_trc20_row and not skip_trc20_this_cycle
        else None
    )

    if trc20_row:
        account = trc20_row["account"]
        symbol = trc20_row["symbol"]
        balance = trc20_row["balance"]
        store_id = trc20_row["store_id"]
        logger.info(
            f"[store_id={store_id}] funds_sweeper: sweeping TRC20 candidate "
            f"{account} {balance} {symbol}"
        )

        result = None
        try:
            if not is_task_running(
                self,
                "app.tasks.transfer_trc20_from",
                args=[account, symbol, store_id],
            ):
                result = transfer_trc20_from(account, symbol, store_id=store_id)
        except Exception as e:
            logger.warning(f"[store_id={store_id}] {account} transfer error: {e}")

        if isinstance(result, dict) and result.get("status") != "error":
            logger.info(
                f"[store_id={store_id}] {account} TRC20 sweep succeeded, "
                f"zeroing out {symbol} balance"
            )
            balance_repository.zero_out(account, symbol)
        else:
            logger.info(
                f"[store_id={store_id}] {account} TRC20 sweep did not complete, "
                "balance left unchanged"
            )

    trx_only_rows = balance_repository.list_trx_only_balances()
    logger.info(f"funds_sweeper: {len(trx_only_rows)} TRX-only account(s) to sweep")
    for row in trx_only_rows:
        account = row["account"]
        store_id = row["store_id"]
        result = None
        try:
            if not is_task_running(
                self,
                "app.tasks.transfer_trx_from",
                args=[account, store_id],
            ):
                # We don't need to check if account has a free bandwidth because tx will raise tronpy.exceptions.ValidationError
                # if there is not enough TRX to burn for bandwidth. We are sending the entire TRX balance,
                # so there will be no TRX to burn for sure.
                result = transfer_trx_from(account, store_id=store_id)
        except Exception as e:
            logger.warning(f"[store_id={store_id}] {account} transfer error: {e}")
            continue
        if isinstance(result, dict) and result.get("status") != "error":
            logger.info(
                f"[store_id={store_id}] {account} TRX sweep succeeded, zeroing out balance"
            )
            balance_repository.zero_out(account, "TRX")
        else:
            logger.info(
                f"[store_id={store_id}] {account} TRX sweep did not complete, "
                "balance left unchanged"
            )

    if trc20_row:
        logger.info("funds_sweeper: TRC20 candidate processed, re-triggering funds_sweeper")
        funds_sweeper.delay()
    else:
        logger.info("funds_sweeper: no TRC20 candidates left, triggering balance_collector")
        balance_collector.delay()


@celery.task(bind=True)
@skip_if_running
def vote_for_sr(self, *args, **kwargs):
    logger.info("Checking voting config")
    if not config.SR_VOTES:
        logger.warning("Voting enabled but no config given. Terminating voting task.")
        return
    logger.info(f"Voting config is OK: {config.SR_VOTES}")
    tron_client = ConnectionManager.client()

    energy_delegator_priv, energy_delegator_pub = get_energy_delegator(
        store_id=1
    )

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
    if config.SR_VOTING:
        vote_for_sr.delay()

    sender.add_periodic_task(config.BALANCES_RESCAN_PERIOD, funds_sweeper.s())
