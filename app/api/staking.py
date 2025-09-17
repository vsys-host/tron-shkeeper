from typing import Literal

from app.utils import get_energy_delegator

from . import staking_bp
from ..db import query_db2
from ..connection_manager import ConnectionManager
from ..wallet_encryption import wallet_encryption
from ..logging import logger

from tronpy import Tron
from tronpy.keys import PrivateKey


@staking_bp.get("/", defaults={"address": None})
@staking_bp.get("/<address>")
def get_resources(address):
    if not address:
        _, address = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    account_info = tron_client.get_account(address)

    index = tron_client.get_delegated_resource_account_index_v2(address)
    account_resource = tron_client.get_account_resource(address)
    delegated_resources = []
    if "toAccounts" in index:
        for to_addr in index["toAccounts"]:
            deleg_res = tron_client.get_delegated_resource_v2(address, to_addr)
            if "delegatedResource" in deleg_res:
                for i in deleg_res["delegatedResource"]:
                    delegated_resources.append(i)

    return {
        "account_info": account_info,
        "delegated_resources": delegated_resources,
        "account_resource": account_resource,
    }


@staking_bp.post("/freeze/<int:amount>/<string:res_type>")
def stake_trx(amount: int, res_type: Literal["ENERGY", "BANDWIDTH"]):
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()

    tron_client: Tron = ConnectionManager.client()
    unsigned_tx = tron_client.trx.freeze_balance(
        owner=energy_delegator_pub,
        amount=amount * 1_000_000,
        resource=res_type,
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    signed_tx.inspect()
    tx_info = signed_tx.broadcast().wait()
    logger.info(tx_info)
    return tx_info


@staking_bp.post("/unfreeze/<int:amount>/<string:res_type>")
def unstake_trx(amount: int, res_type: Literal["ENERGY", "BANDWIDTH"]):
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    unsigned_tx = tron_client.trx.unfreeze_balance(
        owner=energy_delegator_pub,
        resource=res_type,
        unfreeze_balance=amount * 1_000_000,
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    signed_tx.inspect()
    tx_info = signed_tx.broadcast().wait()
    logger.info(tx_info)
    return tx_info


@staking_bp.post("/withdraw_unfreezed")
def withdraw_unstaked_trx():
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    unsigned_tx = tron_client.trx.withdraw_stake_balance(
        owner=energy_delegator_pub
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    signed_tx.inspect()
    tx_info = signed_tx.broadcast().wait()
    logger.info(tx_info)
    return tx_info


@staking_bp.post("/claim_voting_reward")
def claim_voting_reward():
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    unsigned_tx = tron_client.trx.withdraw_rewards(owner=energy_delegator_pub).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    signed_tx.inspect()
    tx_info = signed_tx.broadcast().wait()
    logger.info(tx_info)
    return tx_info


@staking_bp.post("/grant_permissions")
def grant_permissions():
    """
    Allows the fee_deposit account to perform staking and energy
    delegation transactions on behalf of the staking account.

    Prerequisites: The staking account must be active and have
    at least 100 TRX available to cover
    the AccountPermissionUpdate fee (see network parameter #22).
    """
    pass
