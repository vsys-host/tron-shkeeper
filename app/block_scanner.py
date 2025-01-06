import datetime
from decimal import Decimal
import functools
import time
from concurrent.futures import ThreadPoolExecutor

import requests

from tronpy.abi import trx_abi
from eth_abi.exceptions import NonEmptyPaddingBytes, InsufficientDataBytes

from .schemas import TronTransaction

from .config import config
from .db import query_db2
from .logging import logger
from .exceptions import (
    NoServerSet,
    UnknownToken,
    UnknownTransactionType,
    NotificationFailed,
    BadContractResult,
)
from .connection_manager import ConnectionManager


class BlockScanner:
    WATCHED_ACCOUNTS = set()

    def __call__(self):
        with ThreadPoolExecutor(
            max_workers=config.BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE
        ) as executor:
            while True:
                try:
                    blocks = self.get_blocks()
                    if blocks.start == blocks.stop:
                        logger.debug(
                            f"Waiting for a new block for {config.BLOCK_SCANNER_INTERVAL_TIME} seconds."
                        )
                        time.sleep(config.BLOCK_SCANNER_INTERVAL_TIME)
                        continue

                    start_time = time.time()
                    results = list(executor.map(self.scan, blocks))
                    logger.debug(
                        f"Block chunk {blocks.start} - {blocks.stop - 1} processed for {time.time() - start_time} seconds"
                    )

                    if results and all(results):
                        logger.debug(
                            f"Commiting chunk {blocks.start} - {blocks.stop - 1}"
                        )
                        self.set_last_seen_block_num(blocks.stop - 1)
                    else:
                        logger.info(
                            f"Some blocks failed, retrying chunk {blocks.start} - {blocks.stop - 1}"
                        )
                except NoServerSet:
                    time.sleep(1)
                except Exception as e:
                    sleep_sec = 60
                    logger.exception(f"Exteption in main block scanner loop: {e}")
                    logger.warning(f"Waiting {sleep_sec} seconds before retry.")
                    time.sleep(sleep_sec)

    @classmethod
    def get_watched_accounts(cls) -> list:
        return cls.WATCHED_ACCOUNTS

    @classmethod
    def set_watched_accounts(cls, acc_list: list):
        cls.WATCHED_ACCOUNTS = set(acc_list)
        logger.debug(
            f"WATCHED_ACCOUNTS was set. List size: {cls.count_watched_accounts()}"
        )

    @classmethod
    def add_watched_account(cls, acc: str):
        cls.WATCHED_ACCOUNTS.add(acc)
        logger.debug(
            f"Added {acc} to WATCHED_ACCOUNTS. List size: {cls.count_watched_accounts()}"
        )

    @classmethod
    def count_watched_accounts(cls):
        return len(cls.WATCHED_ACCOUNTS)

    @functools.cached_property
    def main_account(self):
        return query_db2('select * from keys where type = "fee_deposit" ', one=True)[
            "public"
        ]

    def get_last_seen_block_num(self) -> int:
        row = query_db2(
            'SELECT value FROM settings WHERE name = "last_seen_block_num"', one=True
        )
        if row:
            last_block_num = int(row["value"])
        else:
            if config.BLOCK_SCANNER_LAST_BLOCK_NUM_HINT:
                last_block_num = int(config.BLOCK_SCANNER_LAST_BLOCK_NUM_HINT)
                logger.info(f"Last seen block is hinted to be {last_block_num}")
            else:
                last_block_num = self.get_current_height()
                logger.info(
                    f"Last seen block is set to full node height {last_block_num}"
                )
            query_db2(
                'INSERT INTO settings VALUES ("last_seen_block_num", ?)',
                (last_block_num,),
            )
        return last_block_num

    def set_last_seen_block_num(self, block_num: int):
        start_time = time.time()
        query_db2(
            'UPDATE settings SET value = ? WHERE name = "last_seen_block_num"',
            (block_num,),
        )
        logger.debug(
            f"set_last_seen_block_num({block_num}) save time: {time.time() - start_time} seconds"
        )

    def get_current_height(self):
        n = ConnectionManager.client().get_latest_block_number()
        logger.debug(f"Block height is {n}")
        return n

    def get_blocks(self):
        last_seen_block_num = self.get_last_seen_block_num()
        next_block = last_seen_block_num + 1
        current_height = self.get_current_height()
        if last_seen_block_num > current_height:
            raise Exception(
                f"Tron fullnode height unexpectedly dropped from {last_seen_block_num} to {current_height}. Refusing to continue."
            )
        target_block = next_block + config.BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE
        if target_block > current_height:
            target_block = current_height
        return range(next_block, target_block + 1)

    @functools.lru_cache(maxsize=config.BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE)
    def download_block(self, n):
        start_time = time.time()
        block = ConnectionManager.client().get_block(n)
        logger.debug(f"Block {n} download took {time.time() - start_time} seconds")
        return block

    def notify_shkeeper(self, symbol, txid):
        url = f"http://{config.SHKEEPER_HOST}/api/v1/walletnotify/{symbol}/{txid}"
        headers = {"X-Shkeeper-Backend-Key": config.SHKEEPER_BACKEND_KEY}
        res = requests.post(url, headers=headers).json()
        logger.info(f"Shkeeper response: {res}")
        if res["status"] != "success":
            raise NotificationFailed(res)

    def scan(self, block_num: int) -> bool:
        from .tasks import transfer_trc20_from, transfer_trx_from
        from .custom.aml.functions import (
            add_transaction_to_db,
        )
        from .custom.aml.tasks import run_payout_for_tx

        try:
            block = self.download_block(block_num)
            if "transactions" not in block:
                logger.debug(f"Block {block_num}: No transactions")
                return True
            start = time.time()
            valid_addresses = self.get_watched_accounts()

            txs = block["transactions"]
            for tx in txs:
                try:
                    tron_tx = parse_tx(tx)
                    logger.debug(f"Block {block_num}: Found {tron_tx=}")

                except (
                    UnknownTransactionType,
                    InsufficientDataBytes,
                    BadContractResult,
                ) as e:
                    logger.debug(f"Can't get info from tx: {e}: {tx}")
                    continue

                except NonEmptyPaddingBytes as e:
                    logger.warning(f"Can't decode tx data: {e}: {tx}")
                    continue

                except Exception as e:
                    logger.warning(
                        f"Block {block_num}: Transaction info extraction error: {e}: {tx}"
                    )
                    raise e

                if config.EXTERNAL_DRAIN_CONFIG:
                    #
                    # Customized workflow (AML)
                    #
                    if tron_tx.dst_addr not in valid_addresses:
                        continue
                    if tron_tx.status != "SUCCESS":
                        logger.warning(
                            f"Skipping notification for bad status TX {tron_tx=}"
                        )
                        continue
                    logger.info(f"Sending notification for TX {tron_tx=}")
                    self.notify_shkeeper(tron_tx.symbol, tron_tx.txid)
                    if (
                        self.main_account not in (tron_tx.src_addr, tron_tx.dst_addr)
                        and tron_tx.dst_addr in valid_addresses
                        and tron_tx.src_addr not in valid_addresses
                    ):  # to one-time from foreign
                        add_transaction_to_db(
                            tron_tx.txid,
                            tron_tx.dst_addr,
                            tron_tx.amount,
                            tron_tx.symbol,
                        )
                        run_payout_for_tx.apply_async(
                            args=[
                                tron_tx.symbol,
                                tron_tx.dst_addr,
                                tron_tx.txid,
                            ],
                            # wait for 5min for data to be updated in AMLBot
                            countdown=config.AML_WAIT_BEFORE_API_CALL,
                        )

                    elif (
                        tron_tx.dst_addr in valid_addresses
                        and tron_tx.src_addr == self.main_account
                    ):  # to one-time from fee-deposit
                        add_transaction_to_db(
                            tron_tx.txid,
                            tron_tx.dst_addr,
                            tron_tx.amount,
                            tron_tx.symbol,
                            "from_fee",
                        )
                    else:
                        raise Exception("")
                else:
                    #
                    # Default workflow
                    #
                    if (
                        tron_tx.symbol == "TRX"
                        and tron_tx.src_addr == self.main_account
                        and tron_tx.dst_addr in valid_addresses
                    ):
                        logger.info(
                            f"Ignoring TRX transaction from main to onetime acc: {tron_tx}"
                        )
                        continue

                    if tron_tx.dst_addr in valid_addresses:
                        if tron_tx.status == "SUCCESS":
                            logger.info(f"Sending notification for {tron_tx}")
                            self.notify_shkeeper(tron_tx.symbol, tron_tx.txid)
                            # Send funds to main account
                            if tron_tx.is_trc20:
                                transfer_trc20_from.delay(
                                    tron_tx.dst_addr, tron_tx.symbol
                                )
                            else:
                                transfer_trx_from.delay(tron_tx.dst_addr)
                        else:
                            logger.warning(
                                f"Not sending notification for tx with status {tron_tx.status}: {tron_tx}"
                            )
            logger.debug(
                f"block {block_num} info extraction time: {time.time() - start}"
            )
        except Exception as e:
            logger.exception(f"Block {block_num}: Failed to scan: {e}")
            return False

        return True


def parse_tx(tx: dict) -> TronTransaction:
    is_trc20 = False
    txid = tx["txID"]
    tx_type = tx["raw_data"]["contract"][0]["type"]
    status = tx["ret"][0]["contractRet"]

    if status != "SUCCESS":
        raise BadContractResult(f"TXID {txid} has result {status}")

    if tx_type == "TransferContract":
        symbol = "TRX"
        from_addr = tx["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"]
        to_addr = tx["raw_data"]["contract"][0]["parameter"]["value"]["to_address"]
        amount = Decimal(
            tx["raw_data"]["contract"][0]["parameter"]["value"]["amount"]
        ) / Decimal(1_000_000)

    elif tx_type == "TriggerSmartContract":
        is_trc20 = True
        cont_addr = tx["raw_data"]["contract"][0]["parameter"]["value"][
            "contract_address"
        ]
        try:
            symbol = config.get_symbol(cont_addr)
        except UnknownToken:
            raise UnknownTransactionType(f"Unknown contract address {cont_addr}")

        raw_data = tx["raw_data"]["contract"][0]["parameter"]["value"]["data"]

        func_selector = raw_data[:8]
        if func_selector != "a9059cbb":  # erc20 transfer()
            raise UnknownTransactionType(f"Unknown function selector: {func_selector}")

        # Workaround for "Can't decode tx data: Padding bytes were not empty" errors
        # https://github.com/ethereum/eth-abi/issues/162
        raw_to_addr = bytes.fromhex("0" * 24 + raw_data[8 + 24 : 8 + 64])
        raw_amount = bytes.fromhex(raw_data[8 + 64 :])
        decoded_amount = trx_abi.decode_single("uint256", raw_amount)

        from_addr = tx["raw_data"]["contract"][0]["parameter"]["value"]["owner_address"]
        to_addr = trx_abi.decode_single("address", raw_to_addr)
        amount = Decimal(decoded_amount) / (10 ** config.get_decimal(symbol))

    else:
        raise UnknownTransactionType(f"Unknown transaction type: {txid}: {tx_type}")

    return TronTransaction(
        status=status,
        txid=txid,
        symbol=symbol,
        src_addr=from_addr,
        dst_addr=to_addr,
        amount=amount,
        is_trc20=is_trc20,
    )


def block_scanner_stats(bs: BlockScanner):
    # waiting for block scanner thread to update settings table
    time.sleep(config.BLOCK_SCANNER_STATS_LOG_PERIOD)

    b_start = bs.get_last_seen_block_num()
    while True:
        try:
            time.sleep(config.BLOCK_SCANNER_STATS_LOG_PERIOD)
            b_now = bs.get_last_seen_block_num()
            ss = (b_now - b_start) / config.BLOCK_SCANNER_STATS_LOG_PERIOD
            b_start = b_now
            h = bs.get_current_height()
            eta = "n/a"
            if ss > 0:
                eta = str(datetime.timedelta(seconds=int((h - b_now) / ss)))
            if abs(h - b_now) <= 1:
                eta = "in sync"
            logger.info(
                f"Stats: scan_bps={ss} | now_block={b_now} | head_block={h} | eta={eta} | accs={bs.count_watched_accounts()}"
            )
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exteption in main scanner stats loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")
            time.sleep(sleep_sec)
