from decimal import Decimal

import tronpy.exceptions
from tronpy.keys import PrivateKey

from .config import config
from .db import query_db2
from .logging import logger
from .connection_manager import ConnectionManager
from .wallet_encryption import wallet_encryption
from .schemas import TronAddress


class Wallet:
    CACHE = {
        "decimals": {},
        "contracts": {},
    }
    main_account = query_db2('select * from keys where type = "fee_deposit" ', one=True)

    def __init__(self, symbol="TRX"):
        self.symbol = symbol
        self.client = ConnectionManager.client()
        if symbol != "TRX":
            self.contract_address = config.get_contract_address(symbol)

    def get_contract(self, contract_address=None):
        if contract_address is None:
            contract_address = self.contract_address
        contract = self.CACHE["contracts"].get(contract_address)
        if not contract:
            contract = self.client.get_contract(contract_address)
            self.CACHE["contracts"][contract_address] = contract
        decimals = self.CACHE["decimals"].get(contract_address)
        if not decimals:
            self.CACHE["decimals"][contract_address] = contract.functions.decimals()
        return contract

    @property
    def balance(self):
        return self.balance_of(self.main_account["public"])

    def balance_of(self, address):
        if self.symbol == "TRX":
            try:
                return self.client.get_account_balance(address)
            except tronpy.exceptions.AddressNotFound:
                return Decimal(0)
        else:
            return (
                Decimal(self.get_contract().functions.balanceOf(address))
                / 10 ** self.CACHE["decimals"][self.contract_address]
            )

    def bandwidth_of(self, address):
        res = self.client.get_account_resource(address)
        logger.debug(f"Resources of {address}: {res}")
        bandwidth = res["freeNetLimit"] - res.get("freeNetUsed", 0)
        return Decimal(bandwidth)

    def transfer(self, dst, amount, src_address: TronAddress = None):
        if src_address:
            src_account = query_db2(
                "select * from keys where public = ?", (src_address,), one=True
            )
        else:
            src_account = self.main_account

        if self.symbol == "TRX":
            txn = self.client.trx.transfer(
                src_account["public"], dst, int(amount * 1_000_000)
            )
        else:
            txn = (
                self.get_contract()
                .functions.transfer(
                    dst, int(amount * (10 ** config.get_decimal(self.symbol)))
                )
                .with_owner(src_account["public"])
                .fee_limit(int(config.TX_FEE_LIMIT * 1_000_000))
            )

        # https://github.com/tronprotocol/java-tron/issues/2883#issuecomment-575007235
        txn._raw_data["expiration"] += 12 * 60 * 60 * 1_000  # 12 hours
        txn = txn.build().sign(
            PrivateKey(bytes.fromhex(wallet_encryption.decrypt(src_account["private"])))
        )
        # logger.debug(f"about to broadcast {txn=}")
        txn_res = txn.broadcast().wait()

        logger.info(
            f"{amount} {self.symbol} has been sent to {dst} with TXID {txn.txid}. Details: {txn_res}"
        )

        result = {
            "dest": dst,
            "amount": str(amount),
            "txids": [txn.txid],
            "details": txn_res,
        }

        if self.symbol == "TRX":
            if txn_res["contractResult"] == [""]:
                result["status"] = "success"
            else:
                result["status"] = "error"
                result["message"] = f"contractResult: {txn_res['contractResult']}"
        else:
            if txn_res["receipt"]["result"] == "SUCCESS":
                result["status"] = "success"
            else:
                result["status"] = "error"
                result["message"] = f"{txn_res['result']}: {txn_res['resMessage']}"

        return result
