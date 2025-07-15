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

from celery.schedules import crontab
from tronpy.keys import PrivateKey
from tronpy.tron import current_timestamp
from tronpy.abi import trx_abi
import tronpy.exceptions
import requests
from sqlmodel import Session, select

from . import celery
from .config import config
from .db import query_db, query_db2
from .wallet import Wallet
from .utils import skip_if_running
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
def transfer_trc20_from(onetime_publ_key, symbol):
    """
    Transfers TRC20 from onetime to main account
    """

    tron_client = ConnectionManager.client()

    contract_address = config.get_contract_address(symbol)
    contract = tron_client.get_contract(contract_address)
    precision = contract.functions.decimals()
    main_acc_keys = query_db2(
        'select * from keys where type = "fee_deposit" ', one=True
    )
    main_priv_key = PrivateKey(
        bytes.fromhex(wallet_encryption.decrypt(main_acc_keys["private"]))
    )
    main_publ_key = main_acc_keys["public"]
    token_balance = contract.functions.balanceOf(onetime_publ_key)

    tx_trx_res = None
    sun_delegated: int | None = None

    def delegate_energy(sun_to_delegate):
        logger.info("Check if main account can delegate energy")
        result = tron_client.provider.make_request(
            "wallet/getcandelegatedmaxsize",
            {"owner_address": main_publ_key, "type": 1, "visible": True},
        )
        if "max_size" not in result:
            raise Exception(
                "Main account has no delegatable energy. Terminating transfer."
            )
        else:
            delegetable_sun = result["max_size"]

            logger.info(f"{delegetable_sun=} {sun_to_delegate=}")

            if delegetable_sun < sun_to_delegate:
                raise Exception(
                    "Main account has not enough energy. Terminating transfer."
                )
            else:
                logger.info("Main account has enough energy")

                logger.info("Delegating energy to onetime account")

                unsigned_tx = tron_client.trx.delegate_resource(
                    owner=main_publ_key,
                    receiver=onetime_publ_key,
                    balance=sun_to_delegate,
                    resource="ENERGY",
                ).build()
                signed_tx = unsigned_tx.sign(main_priv_key)
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
                    raise Exception(
                        "Onetime account has not enough energy after delegation. Terminating transfer."
                    )
                else:
                    logger.info("Energy successfuly delegated")

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

        logger.info("Check main account bandwidth")
        main_acc_res = tron_client.get_account_resource(main_publ_key)
        free_bandwidth_available = main_acc_res["freeNetLimit"] - main_acc_res.get(
            "freeNetUsed", 0
        )
        staked_bandwidth_available = main_acc_res["NetLimit"] - main_acc_res.get(
            "NetUsed", 0
        )
        logger.info(
            f"Main account: {staked_bandwidth_available=} {free_bandwidth_available=}"
        )
        logger.debug(f"{main_acc_res=}")

        need_bw = (
            config.BANDWIDTH_PER_DELEGE_CALL
            + config.BANDWIDTH_PER_UNDELEGATE_CALL
            + config.BANDWIDTH_PER_TRX_TRANSFER
        )
        logger.info(f"Estimated bandwidth requirement: {need_bw}")

        if staked_bandwidth_available < need_bw:
            logger.info(
                f"Not enough staked bandwidth. Has: {staked_bandwidth_available} Need: {need_bw}"
            )
            if free_bandwidth_available < need_bw:
                logger.info(
                    f"Not enough free bandwidth. Has: {free_bandwidth_available} Need: {need_bw}"
                )
                if config.ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_FOR_BANDWITH:
                    logger.info("Burning TRX for bandwidth.")
                else:
                    logger.warning(
                        "Main account has not enough staked or free bandwidth to procced. Terminating transfer."
                    )
                    return
            else:
                logger.info(
                    f"Using free bandwidth. Has: {free_bandwidth_available} Need: {need_bw}"
                )
        else:
            logger.info(
                f"Using staked bandwidth. Has: {staked_bandwidth_available} Need: {need_bw}"
            )

        try:
            onetime_address_resources = tron_client.get_account_resource(
                onetime_publ_key
            )
            logger.info(
                f"Onetime {onetime_publ_key} is already on chain, skipping actication. Resource details {onetime_address_resources=}"
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
        trx_needed = (
            onetime_address_resources.get("TotalEnergyWeight") * energy_needed
        ) / onetime_address_resources.get("TotalEnergyLimit")

        sun_needed = math.ceil(trx_needed) * 1_000_000

        logger.info(
            f"{onetime_publ_key=} {onetime_energy_available=} {energy_needed=} {trx_needed=} {sun_needed=}"
        )

        if onetime_energy_available >= energy_needed:
            logger.info("Onetime account has enough energy for transfer.")

        else:
            logger.info(
                "Onetime account hasn't enough energy for transfer. Delegating energy to onetime account"
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
                        additional_trx_needed = (
                            onetime_address_resources.get("TotalEnergyWeight")
                            * energy_diff
                        ) / onetime_address_resources.get("TotalEnergyLimit")
                        additional_sun_needed = (
                            math.ceil(additional_trx_needed) * 1_000_000
                        )
                        logger.info(
                            "Energy diff is {energy_diff}. TRX to delegate: {additional_trx_needed}"
                        )
                        delegate_energy(additional_sun_needed)
                        sun_delegated = additional_sun_needed

                    else:
                        logger.warning("Terminating transfer.")
                        return
            else:
                logger.info("No delagated energy found")
                delegate_energy(sun_needed)
                sun_delegated = sun_needed
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
            undelegate_energy(onetime_publ_key, sun_delegated)
        else:
            undelegate_energy.delay(onetime_publ_key, sun_delegated)

    return {"tx_trx_res": tx_trx_res, "tx_token": tx_token_res}


@celery.task()
def undelegate_energy(receiver, balance):
    logger.info("Undelegating energy from onetime account")

    tron_client = ConnectionManager.client()

    main_acc_keys = query_db2(
        'select * from keys where type = "fee_deposit" ', one=True
    )
    main_priv_key = PrivateKey(
        bytes.fromhex(wallet_encryption.decrypt(main_acc_keys["private"]))
    )
    main_publ_key = main_acc_keys["public"]

    unsigned_tx = tron_client.trx.undelegate_resource(
        owner=main_publ_key,
        receiver=receiver,
        balance=balance,
        resource="ENERGY",
    ).build()
    signed_tx = unsigned_tx.sign(main_priv_key)
    undelegate_tx_info = signed_tx.broadcast().wait()

    logger.info(
        f"Undelegated resources from onetime account {receiver} with TXID: {unsigned_tx.txid}"
    )
    logger.info(undelegate_tx_info)


@celery.task()
def transfer_trx_from(onetime_publ_key):
    """
    Transfers TRX from onetime to main account
    """

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

    main_publ_key = query_db2(
        'select * from keys where type = "fee_deposit" ', one=True
    )["public"]

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

        @cache
        def precision_of(symbol):
            return (
                ConnectionManager.client()
                .get_contract(config.get_contract_address(symbol))
                .functions.decimals()
            )

        accounts = [
            row["public"]
            for row in query_db('SELECT public FROM keys WHERE type = "onetime"')
        ]
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
                        if not is_task_running(
                            self,
                            "app.tasks.transfer_trc20_from",
                            args=[account, symbol],
                        ):
                            transfer_trc20_from(account, symbol)

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
                    if not is_task_running(
                        self, "app.tasks.transfer_trc20_from", args=[account]
                    ):
                        # We don't need to check if account has a free bandwidth because tx will raise tronpy.exceptions.ValidationError
                        # if there is not enough TRX to burn for bandwidth. We are sending the entire TRX balance,
                        # so there will be no TRX to burn for sure.
                        transfer_trx_from(account)

                logger.debug(
                    f"Scanned {index} of {len(accounts)} accounts, found: "
                    + ", ".join([f"{v} {k}" for k, v in stats["balances"].items()])
                )

            except Exception as e:
                logger.exception(f"{account} scan error: {e}")
                stats["exception_num"] += 1
    return stats


@celery.on_after_configure.connect
def setup_periodic_tasks(sender, **kwargs):
    if config.EXTERNAL_DRAIN_CONFIG:
        from .custom.aml.tasks import sweep_accounts, recheck_transactions

        sender.add_periodic_task(
            config.AML_RESULT_UPDATE_PERIOD, recheck_transactions.s()
        )
        sender.add_periodic_task(config.AML_SWEEP_ACCOUNTS_PERIOD, sweep_accounts.s())
    else:
        sender.add_periodic_task(config.BALANCES_RESCAN_PERIOD, scan_accounts.s())
