from collections import namedtuple
import datetime
from decimal import Decimal
import functools
import time
from typing import Tuple
from concurrent.futures import ThreadPoolExecutor

import requests

from tronpy.abi import trx_abi
from eth_abi.exceptions import NonEmptyPaddingBytes, InsufficientDataBytes

from .config import config, get_symbol
from .db import query_db2
from .logging import logger
from .utils import get_tron_client
from .exceptions import UnknownTransactionType, NotificationFailed


class BlockScanner:

    def __init__(self) -> None:
        self.tron_client = get_tron_client()

        if self.get_last_seen_block_num() is None:
            if config['BLOCK_SCANNER_LAST_BLOCK_NUM_HINT']:
                last_block_num = int(config['BLOCK_SCANNER_LAST_BLOCK_NUM_HINT'])
            else:
                last_block_num = self.get_current_height()
            query_db2('INSERT INTO settings VALUES ("last_seen_block_num", ?)', (last_block_num,))

    def __call__(self):
        with ThreadPoolExecutor(max_workers=config['BLOCK_SCANNER_MAX_BLOCK_CHUNK_SIZE']) as executor:
            while True:
                try:
                    blocks =  self.get_blocks()
                    if blocks.start == blocks.stop:
                        logger.debug(f"Waiting for a new block for {config['BLOCK_SCANNER_INTERVAL_TIME']} seconds.")
                        time.sleep(config['BLOCK_SCANNER_INTERVAL_TIME'])
                        continue

                    results = list(executor.map(self.scan, blocks))
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

    def get_last_seen_block_num(self) -> int:
        row = query_db2('SELECT value FROM settings WHERE name = "last_seen_block_num"', one=True)
        return int(row['value']) if row['value'] else None

    def set_last_seen_block_num(self, block_num: int):
        query_db2('UPDATE settings SET value = ? WHERE name = "last_seen_block_num"', (block_num,))

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
        logger.debug(f'Block {n}: DOWNLOAD')
        return self.tron_client.get_block(n)

    def notify_shkeeper(self, symbol, txid):
        url = f'http://{config["SHKEEPER_HOST"]}/api/v1/walletnotify/{symbol}/{txid}'
        headers = {'X-Shkeeper-Backend-Key': config['SHKEEPER_KEY']}
        res = requests.post(url, headers=headers).json()
        logger.info(f'Shkeeper response: {res}')
        if res['status'] != 'success':
            raise NotificationFailed(res)

    def scan(self, block_num: int) -> bool:
        try:
            block = self.download_block(block_num)
            if not 'transactions' in block:
                logger.debug(f"Block {block_num}: No transactions")
                return True

            valid_addresses = [row['public']
                               for row in query_db2('select public from keys where type = "onetime"')]

            txs = block['transactions']
            for tx in txs:
                try:
                    info = self.get_tx_info(tx)
                    logger.debug(f"Block {block_num}: Found transaction {info.txid}")

                except InsufficientDataBytes as e:
                    logger.debug(f"Can't get info from tx: {e}: {tx}")
                    continue

                except UnknownTransactionType as e:
                    logger.debug(f"Can't get info from tx: {e}: {tx}")
                    continue

                except NonEmptyPaddingBytes as e:
                    logger.warning(f"Can't decode tx data: {e}: {tx}")
                    continue

                except Exception as e:
                    logger.warning(f"Block {block_num}: Transaction info extraction error: {e}: {tx}")
                    raise e

                if info.to_addr in valid_addresses:
                    if info.status == 'SUCCESS':
                        logger.info(f"Sending notification for {info}")
                        self.notify_shkeeper(info.symbol, info.txid)
                    else:
                        logger.warning(f"Not sending notification for tx with status {info.status}: {info}")

        except Exception as e:
            logger.exception(f'Block {block_num}: Failed to scan: {e}')
            return False

        return True

    @staticmethod
    def get_tx_info(tx: dict) -> Tuple[str, str, str, str, Decimal]:

        txid = tx['txID']
        tx_type = tx['raw_data']['contract'][0]['type']
        status = tx['ret'][0]['contractRet']

        if tx_type == 'TransferContract':
            symbol = 'TRX'
            from_addr = tx['raw_data']['contract'][0]['parameter']['value']['owner_address']
            to_addr = tx['raw_data']['contract'][0]['parameter']['value']['to_address']
            amount = Decimal(tx['raw_data']['contract'][0]['parameter']['value']['amount']) / Decimal(1e9)

        elif tx_type == 'TriggerSmartContract':
            cont_addr = tx['raw_data']['contract'][0]['parameter']['value']['contract_address']
            try:
                symbol = get_symbol(cont_addr)
            except KeyError:
                raise UnknownTransactionType(f'Unknown contract address {cont_addr}')

            func_selector = tx['raw_data']['contract'][0]['parameter']['value']['data'][:8]
            if func_selector != 'a9059cbb':  # erc20 transfer()
                raise UnknownTransactionType(f'Unknown function selector: {func_selector}')

            from_addr = tx['raw_data']['contract'][0]['parameter']['value']['owner_address']
            to_addr, raw_amount = trx_abi.decode_abi(
                ['address', 'uint256'],
                bytes.fromhex(tx['raw_data']['contract'][0]['parameter']['value']['data'][8:])
            )
            amount = Decimal(raw_amount) / Decimal(1e6)

        else:
            raise UnknownTransactionType(f'Unknown transaction type: {txid}: {tx_type}')

        return TxInfo(status, txid, symbol, from_addr, to_addr, amount)


def block_scanner_stats(bs: BlockScanner):
    b_start = bs.get_last_seen_block_num()
    while True:
        try:
            time.sleep(config['BLOCK_SCANNER_STATS_LOG_PERIOD'])
            b_now = bs.get_last_seen_block_num()
            ss = (b_now - b_start) / config['BLOCK_SCANNER_STATS_LOG_PERIOD']
            b_start = b_now
            h = bs.get_current_height()
            if ss > 0:
                eta = str(datetime.timedelta(seconds=int((h - b_now) / ss)))
            if abs(h -  b_now) <= 1:
                eta = 'in sync'
            logger.info(f"Stats: scan_bps={ss} | now_block={b_now} | head_block={h} | eta={eta}")
        except Exception as e:
            sleep_sec = 60
            logger.exception(f"Exteption in main scanner stats loop: {e}")
            logger.warning(f"Waiting {sleep_sec} seconds before retry.")
            time.sleep(sleep_sec)

TxInfo = namedtuple('TxInfo', 'status, txid, symbol, from_addr, to_addr, amount')