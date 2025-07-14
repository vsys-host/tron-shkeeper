from typing import Literal

from . import staking_bp
from ..db import query_db2
from ..connection_manager import ConnectionManager
from ..wallet_encryption import wallet_encryption
from ..logging import logger

from tronpy import Tron
from tronpy.keys import PrivateKey


@staking_bp.post("/freeze/<int:amount>/<string:res_type>")
def stake_trx(amount: int, res_type: Literal["ENERGY", "BANDWIDTH"]):
    main_acc_keys = query_db2(
        'select * from keys where type = "fee_deposit" ', one=True
    )
    main_priv_key = PrivateKey(
        bytes.fromhex(wallet_encryption.decrypt(main_acc_keys["private"]))
    )
    main_publ_key = main_acc_keys["public"]

    tron_client: Tron = ConnectionManager.client()
    unsigned_tx = tron_client.trx.freeze_balance(
        owner=main_publ_key,
        amount=amount * 1_000_000,
        resource=res_type,
    ).build()
    signed_tx = unsigned_tx.sign(main_priv_key)
    signed_tx.inspect()
    tx_info = signed_tx.broadcast().wait()
    logger.info(tx_info)
    return tx_info
