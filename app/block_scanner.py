import datetime
from decimal import Decimal
import functools
import time
from concurrent.futures import ThreadPoolExecutor
from typing import List

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
                        chunk_retry_sleep_period = 5
                        logger.info(
                            f"Some blocks failed, retrying chunk {blocks.start} - {blocks.stop - 1} after {chunk_retry_sleep_period}s"
                        )
                        time.sleep(chunk_retry_sleep_period)
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

    @functools.lru_cache(maxsize=config.BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE)
    def download_tx_info_by_block_num(self, n):
        start_time = time.time()
        transaction_results = ConnectionManager.client().provider.make_request(
            "wallet/gettransactioninfobyblocknum", {"num": n, "visible": True}
        )
        logger.debug(
            f"Tx info for block {n} download took {time.time() - start_time} seconds"
        )
        return {
            result["id"]: result for result in transaction_results if "log" in result
        }

    def notify_shkeeper(self, symbol, txid):
        if config.DEVMODE_SKIP_NOTIFICATIONS:
            logger.info(f"[DEVMODE] Skipping notification for TXID {txid}")
            return

        url = f"http://{config.SHKEEPER_HOST}/api/v1/walletnotify/{symbol}/{txid}"
        headers = {"X-Shkeeper-Backend-Key": config.SHKEEPER_BACKEND_KEY}
        res = requests.post(url, headers=headers).json()
        logger.info(f"Shkeeper response: {res}")
        if res["status"] != "success":
            raise NotificationFailed(res)

    def scan(self, block_num: int) -> bool:
        try:
            block = self.download_block(block_num)
            if "transactions" not in block:
                logger.debug(f"Block {block_num}: No transactions")
                return True

            block_tx_info = self.download_tx_info_by_block_num(block_num)

            start = time.time()
            valid_addresses = self.get_watched_accounts()

            txs = block["transactions"]
            for tx in txs:
                try:
                    tx_info = block_tx_info.get(tx["txID"], {})
                    tron_tx_list = parse_tx(tx, tx_info)
                    logger.debug(f"Block {block_num}: Found {tron_tx_list=}")

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

                for tron_tx in tron_tx_list:
                    #
                    # Custom AML2 workflow
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
                            self.notify_shkeeper(tron_tx.symbol.value, tron_tx.txid)
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


def parse_tx(tx: dict, transaction_info) -> List[TronTransaction]:
    transactions = []
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

        transactions.append(
            TronTransaction(
                status=status,
                txid=txid,
                symbol=symbol,
                src_addr=from_addr,
                dst_addr=to_addr,
                amount=amount,
                is_trc20=is_trc20,
            )
        )

    elif tx_type == "TriggerSmartContract":
        is_trc20 = True

        if "log" not in transaction_info:
            raise UnknownTransactionType(f"Transaction {txid} produced no logs")

        for entry in transaction_info["log"]:
            try:
                log_entry_producer_address = entry["address"]
                symbol = config.get_symbol(log_entry_producer_address)
            except UnknownToken:
                continue

            # >>> from tronpy.contract import keccak256
            # >>> bytes.hex(keccak256("Transfer(address,address,uint256)".encode()))
            # 'ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef'
            transfer_event = (
                "ddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"
            )
            event = entry["topics"][0]
            if event != transfer_event:
                continue

            _, hex_from_addr, hex_to_addr = entry["topics"]

            from_addr = trx_abi.decode_single(
                "address",
                bytes.fromhex(hex_from_addr),
            )
            to_addr = trx_abi.decode_single(
                "address",
                bytes.fromhex(hex_to_addr),
            )
            decoded_amount = trx_abi.decode_single(
                "uint256", bytes.fromhex(entry["data"])
            )
            amount = Decimal(decoded_amount) / (10 ** config.get_decimal(symbol))

            transactions.append(
                TronTransaction(
                    status=status,
                    txid=txid,
                    symbol=symbol,
                    src_addr=from_addr,
                    dst_addr=to_addr,
                    amount=amount,
                    is_trc20=is_trc20,
                )
            )
    else:
        raise UnknownTransactionType(f"Unknown transaction type: {txid}: {tx_type}")

    return transactions


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
