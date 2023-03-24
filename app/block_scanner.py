from collections import namedtuple
from dataclasses import dataclass
import datetime
from decimal import Decimal
import functools
import time
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor

import requests

from tronpy.abi import trx_abi
from eth_abi.exceptions import NonEmptyPaddingBytes, InsufficientDataBytes

from .config import config, get_symbol
from .db import query_db2
from .logging import logger
from .utils import get_tron_client
from .exceptions import UnknownTransactionType, NotificationFailed, BadContractResult


class BlockScanner:

    WATCHED_ACCOUNTS = set()

    def __init__(self) -> None:
        self.tron_client = get_tron_client()

    def __call__(self):
        num = self.get_last_seen_block_num()
        logger.info(f'Last seen block number is {num}')
        logger.info(f'Concurrency is set to {config["BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE"]}')

        with ThreadPoolExecutor(max_workers=config['BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE']) as executor:
            while True:
                try:
                    blocks =  self.get_blocks()
                    if blocks.start == blocks.stop:
                        logger.debug(f"Waiting for a new block for {config['BLOCK_SCANNER_INTERVAL_TIME']} seconds.")
                        time.sleep(config['BLOCK_SCANNER_INTERVAL_TIME'])
                        continue

                    start_time = time.time()
                    results = list(executor.map(self.scan, blocks))
                    logger.debug(f'Block chunk {blocks.start} - {blocks.stop - 1} processed for {time.time() - start_time} seconds')

                    if all(results):
                        logger.debug(f"Commiting chunk {blocks.start} - {blocks.stop - 1}")
                        self.set_last_seen_block_num(blocks.stop - 1)
                    else:
                        logger.info(f"Some blocks failed, retrying chunk {blocks.start} - {blocks.stop - 1}")

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
        logger.debug(f'WATCHED_ACCOUNTS was set. List size: {cls.count_watched_accounts()}')

    @classmethod
    def add_watched_account(cls, acc: str):
        cls.WATCHED_ACCOUNTS.add(acc)
        logger.debug(f'Added {acc} to WATCHED_ACCOUNTS. List size: {cls.count_watched_accounts()}')

    @classmethod
    def count_watched_accounts(cls):
        return len(cls.WATCHED_ACCOUNTS)

    @functools.cached_property
    def main_account(self):
        return query_db2('select * from keys where type = "fee_deposit" ', one=True)['public']

    def get_last_seen_block_num(self) -> int:
        row = query_db2('SELECT value FROM settings WHERE name = "last_seen_block_num"', one=True)
        if row:
            last_block_num = int(row['value'])
        else:
            if config['BLOCK_SCANNER_LAST_BLOCK_NUM_HINT']:
                last_block_num = int(config['BLOCK_SCANNER_LAST_BLOCK_NUM_HINT'])
                logger.info(f'Last seen block is hinted to be {last_block_num}')
            else:
                last_block_num = self.get_current_height()
                logger.info(f'Last seen block is set to full node height {last_block_num}')
            query_db2('INSERT INTO settings VALUES ("last_seen_block_num", ?)', (last_block_num,))
        return last_block_num

    def set_last_seen_block_num(self, block_num: int):
        start_time = time.time()
        query_db2('UPDATE settings SET value = ? WHERE name = "last_seen_block_num"', (block_num,))
        logger.debug(f'set_last_seen_block_num({block_num}) save time: {time.time() - start_time} seconds')

    def get_current_height(self):
        n = self.tron_client.get_latest_block_number()
        logger.debug(f'Block height is {n}')
        return n

    def get_blocks(self):
        next_block = self.get_last_seen_block_num() + 1
        current_height = self.get_current_height()
        target_block = next_block + config['BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE']
        if target_block > current_height:
            target_block = current_height
        return range(next_block, target_block + 1)

    @functools.lru_cache(maxsize=config['BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE'])
    def download_block(self, n):
        start_time = time.time()
        block = self.tron_client.get_block(n)
        logger.debug(f'Block {n} download took {time.time() - start_time} seconds')
        return block

    def notify_shkeeper(self, symbol, txid):
        url = f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}'
        headers = {'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}
        res = requests.post(url, headers=headers).json()
        logger.info(f'Shkeeper response: {res}')
        if res['status'] != 'success':
            raise NotificationFailed(res)

    def scan(self, block_num: int) -> bool:
        from .tasks import transfer_trc20_tokens_to_main_account, transfer_trx_to_main_account

        try:
            block = self.download_block(block_num)
            if not 'transactions' in block:
                logger.debug(f"Block {block_num}: No transactions")
                return True
            start = time.time()
            valid_addresses = self.get_watched_accounts()

            txs = block['transactions']
            for tx in txs:
                try:
                    info = self.get_tx_info(tx)
                    logger.debug(f"Block {block_num}: Found transaction {info.txid}")

                except (UnknownTransactionType, InsufficientDataBytes, BadContractResult) as e:
                    logger.debug(f"Can't get info from tx: {e}: {tx}")
                    continue

                except NonEmptyPaddingBytes as e:
                    logger.warning(f"Can't decode tx data: {e}: {tx}")
                    continue

                except Exception as e:
                    logger.warning(f"Block {block_num}: Transaction info extraction error: {e}: {tx}")
                    raise e

                if info.symbol == 'TRX' and info.from_addr == self.main_account \
                                        and info.to_addr in valid_addresses:
                    logger.info(f"Ignoring TRX transaction from main to onetime acc: {info}")
                    continue

                if info.to_addr in valid_addresses:
                    if info.status == 'SUCCESS':
                        logger.info(f"Sending notification for {info}")

                        self.notify_shkeeper(info.symbol, info.txid)

                        # Send funds to main account
                        if info.is_trc20:
                            transfer_trc20_tokens_to_main_account.delay(info.to_addr, info.symbol)
                        else:
                            transfer_trx_to_main_account.delay(info.to_addr)
                    else:
                        logger.warning(f"Not sending notification for tx with status {info.status}: {info}")
            logger.debug(f"block {block_num} info extraction time: {time.time() - start}")
        except Exception as e:
            logger.exception(f'Block {block_num}: Failed to scan: {e}')
            return False

        return True

    @staticmethod
    def get_tx_info(tx: dict) -> 'TxInfo':
        is_trc20 = False
        txid = tx['txID']
        tx_type = tx['raw_data']['contract'][0]['type']
        status = tx['ret'][0]['contractRet']

        if status != 'SUCCESS':
            raise BadContractResult(f'TXID {txid} has result {status}')

        if tx_type == 'TransferContract':
            symbol = 'TRX'
            from_addr = tx['raw_data']['contract'][0]['parameter']['value']['owner_address']
            to_addr = tx['raw_data']['contract'][0]['parameter']['value']['to_address']
            amount = Decimal(tx['raw_data']['contract'][0]['parameter']['value']['amount']) / Decimal(1_000_000)

        elif tx_type == 'TriggerSmartContract':
            is_trc20 = True
            cont_addr = tx['raw_data']['contract'][0]['parameter']['value']['contract_address']
            try:
                symbol = get_symbol(cont_addr)
            except KeyError:
                raise UnknownTransactionType(f'Unknown contract address {cont_addr}')

            raw_data = tx['raw_data']['contract'][0]['parameter']['value']['data']

            func_selector = raw_data[:8]
            if func_selector != 'a9059cbb':  # erc20 transfer()
                raise UnknownTransactionType(f'Unknown function selector: {func_selector}')

            # Workaround for "Can't decode tx data: Padding bytes were not empty" errors
            # https://github.com/ethereum/eth-abi/issues/162
            raw_to_addr = bytes.fromhex('0' * 24 + raw_data[8+24:8+64])
            raw_amount = bytes.fromhex(raw_data[8+64:])
            decoded_amount = trx_abi.decode_single('uint256', raw_amount)

            from_addr = tx['raw_data']['contract'][0]['parameter']['value']['owner_address']
            to_addr = trx_abi.decode_single('address', raw_to_addr)
            amount = Decimal(decoded_amount) / Decimal(1e6)

        else:
            raise UnknownTransactionType(f'Unknown transaction type: {txid}: {tx_type}')

        return TxInfo(status, txid, symbol, from_addr, to_addr, amount, is_trc20)


def block_scanner_stats(bs: BlockScanner):

    # waiting for block scanner thread to update settings table
    time.sleep(config['BLOCK_SCANNER_STATS_LOG_PERIOD'])

    b_start = bs.get_last_seen_block_num()
    while True:
        try:
            time.sleep(config['BLOCK_SCANNER_STATS_LOG_PERIOD'])
            b_now = bs.get_last_seen_block_num()
            ss = (b_now - b_start) / config['BLOCK_SCANNER_STATS_LOG_PERIOD']
            b_start = b_now
            h = bs.get_current_height()
            eta = 'n/a'
            if ss > 0:
                eta = str(datetime.timedelta(seconds=int((h - b_now) / ss)))
            if abs(h -  b_now) <= 1:
                eta = 'in sync'
            logger.info(f"Stats: scan_bps={ss} | now_block={b_now} | head_block={h} | eta={eta} | accs={bs.count_watched_accounts()}")
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exteption in main scanner stats loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")
            time.sleep(sleep_sec)

# TxInfo = namedtuple('TxInfo', 'status, txid, symbol, from_addr, to_addr, amount, is_trc20')

@dataclass
class TxInfo:
    status: str
    txid: str
    symbol: str
    from_addr: str
    to_addr: str
    amount: Decimal
    is_trc20: bool