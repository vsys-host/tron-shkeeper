from decimal import Decimal

import tronpy
from sqlmodel import Session

from ...config import config
from ...db import engine
from ...logging import logger
from ...wallet import Wallet
from .models import Payout
from ...utils import short_txid


class AmlWallet(Wallet):
    def __init__(self, symbol="TRX"):
        super().__init__(symbol)

    def payout_for_tx(self, tx_id, account):
        from .functions import build_payout_list

        drain_results = []

        external_drain_list = build_payout_list(self.symbol, tx_id)
        logger.debug(f"{external_drain_list=}")
        if not external_drain_list:
            return False

        account_balance = self.balance_of(account)
        logger.debug(f"{account_balance=} {self.symbol=}")

        if "TRX" == self.symbol:
            #
            # TRX workflow
            #
            logger.debug("TRX workflow")
            if account_balance < config.TRX_MIN_TRANSFER_THRESHOLD:
                # logger.warning(
                #     f"Balance {account_balance} is lower "
                #     f"than {config.TRX_MIN_TRANSFER_THRESHOLD=}, skip draining"
                # )
                return False
            logger.debug(f"{config.TRX_MIN_TRANSFER_THRESHOLD=} passed")

            bandwidth_cost = (
                config.TRX_PER_BANDWIDTH_UNIT * config.BANDWIDTH_PER_TRX_TRANSFER
            )
            total_payout_bandwidth_cost = bandwidth_cost * len(external_drain_list)
            for i in range(len(external_drain_list)):
                external_drain_list[i][1] = external_drain_list[i][1] - bandwidth_cost

            total_payout_sum = Decimal(0)
            for payout_destination in external_drain_list:
                dst_addr, amount, orig_amount = payout_destination
                total_payout_sum += amount
                logger.debug(f"{dst_addr=} {amount=} {total_payout_sum=}")

            if (total_payout_sum + total_payout_bandwidth_cost) > account_balance:
                logger.warning(
                    f"Need to drain bigger amount {total_payout_sum}"
                    f"than have in balance {account_balance}, skip draining "
                )
                return False
            logger.debug(f"{total_payout_sum=} <= {account_balance=}")
        else:
            #
            # TRC20 token workflow
            #
            logger.debug("TRC20 workflow")
            if account_balance < config.get_min_transfer_threshold(self.symbol):
                # logger.warning(
                #     f"Balance {account_balance} is less than minimal transfer"
                #     f"threshold of {config.get_min_transfer_threshold(self.symbol)}, skip draining"
                # )
                return False
            logger.debug(f"{config.get_min_transfer_threshold(self.symbol)=} passed")

            total_trx_fee = config.INTERNAL_TX_FEE * len(external_drain_list)
            logger.debug(f"{total_trx_fee=}")
            trx_wallet = Wallet()
            trx_balance = trx_wallet.balance_of(account)
            trx_fee_delta = total_trx_fee - trx_balance
            logger.debug(f"{trx_fee_delta=} = {total_trx_fee=} - {trx_balance=}")
            if trx_fee_delta > 0:
                #
                # Transfer trx_fee_delta TRX from fee-deposit to one-time account
                #
                logger.debug(f"Transfering {trx_fee_delta=} TRX to {account=}")
                result = trx_wallet.transfer(account, trx_fee_delta)
                logger.debug(
                    f"Transfered {trx_fee_delta=} TRX from fee-deposit "
                    f"to one-time {account=} with {result=}"
                )
                if result["status"] != "success":
                    logger.error(f"Transfer error: {result=}")
                    return False
                # logger.debug(
                #     f"Waiting {config.DELAY_AFTER_FEE_TRANSFER=} after successful fee transaction"
                # )
                # time.sleep(config.DELAY_AFTER_FEE_TRANSFER)

        #
        # Same workflow for both TRX and TRC20 token
        #
        logger.info(f"{short_txid(tx_id)} payout started")
        for payout_destination in external_drain_list:
            dst_addr, amount, orig_amount = payout_destination
            logger.debug(
                f"Transfering {amount=} {self.symbol=} from {account=} to {dst_addr=}"
            )
            logger.debug(f"{account=} {self.bandwidth_of(account)=}")
            try:
                res = self.transfer(dst_addr, amount, src_address=account)
            except tronpy.exceptions.ValidationError as e:
                logger.error(f"error: {e}")
                logger.error(
                    f"balance of {account} is {self.balance_of(account)}, bandwidth is {self.bandwidth_of(account)}"
                )
                return False
            logger.debug(f"Transfer result {res=}")

            with Session(engine) as session:
                payout = Payout(
                    external_tx_id=res["txids"][0],
                    tx_id=tx_id,
                    address=dst_addr,
                    crypto=self.symbol,
                    amount_calc=orig_amount,
                    amount_send=amount,
                    status=res["status"],
                )
                logger.debug(f"Writing payout to DB: {payout}...")
                session.add(payout)
                session.commit()
                session.refresh(payout)
                logger.debug("Writing payout to DB: done!")

            drain_results.append(
                {
                    "dest": payout.address,
                    "amount": amount,
                    "status": res["status"],
                    "txids": res["txids"],
                }
            )
            # time.sleep(10)  # FIXME

        for payout in drain_results:
            logger.info(
                f"{short_txid(tx_id)} payment sent: {payout['amount']} {self.symbol} -> {payout['dest']} ({short_txid(payout['txids'][0])})"
            )
        logger.info(f"{short_txid(tx_id)} payout complete")
        return drain_results
