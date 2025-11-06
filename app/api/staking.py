from typing import Literal


from app.utils import get_energy_delegator, get_key
from app.schemas import KeyType

from . import staking_bp
from ..db import query_db2
from ..connection_manager import ConnectionManager
from ..wallet_encryption import wallet_encryption
from ..logging import logger
from ..config import config

from tronpy import Tron
from tronpy.keys import PrivateKey
from tronpy.exceptions import AddressNotFound


@staking_bp.get("/info")
def get_staking_info():
    """
    Returns staking-related configuration options and account status information.

    Returns:
        dict: Contains:
            - config: Staking-related configuration options
            - fee_deposit_account: Fee deposit account address and status (on-chain/off-chain)
            - energy_delegator_account: Energy delegator account address and status (on-chain/off-chain)
    """
    try:
        tron_client: Tron = ConnectionManager.client()

        # Get fee deposit account
        _, fee_deposit_address = get_key(KeyType.fee_deposit)
        fee_deposit_status = "unknown"
        fee_deposit_info = None

        if fee_deposit_address:
            try:
                fee_deposit_info = tron_client.get_account(fee_deposit_address)
                fee_deposit_status = True
            except AddressNotFound:
                fee_deposit_status = False

        # Get energy delegator account (might be different from fee_deposit)
        energy_delegator_priv, energy_delegator_address = get_energy_delegator()
        energy_delegator_status = None
        energy_delegator_info = None

        if energy_delegator_address:
            try:
                energy_delegator_info = tron_client.get_account(
                    energy_delegator_address
                )
                energy_delegator_status = True
            except AddressNotFound:
                energy_delegator_status = False

        # Collect staking-related configuration
        staking_config = {
            "energy_delegation_mode": config.ENERGY_DELEGATION_MODE,
            "energy_delegation_mode_allow_burn_trx_for_bandwidth": config.ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_FOR_BANDWITH,
            "energy_delegation_mode_allow_burn_trx_on_payout": config.ENERGY_DELEGATION_MODE_ALLOW_BURN_TRX_ON_PAYOUT,
            "energy_delegation_mode_allow_additional_energy_delegation": config.ENERGY_DELEGATION_MODE_ALLOW_ADDITIONAL_ENERGY_DELEGATION,
            "energy_delegation_mode_energy_delegation_factor": float(
                config.ENERGY_DELEGATION_MODE_ENERGY_DELEGATION_FACTOR
            ),
            "energy_delegation_mode_separate_balance_and_energy_accounts": config.ENERGY_DELEGATION_MODE_SEPARATE_BALANCE_AND_ENERGY_ACCOUNTS,
            "energy_delegation_mode_energy_account_pub_key": config.ENERGY_DELEGATION_MODE_ENERGY_ACCOUNT_PUB_KEY,
            "sr_voting": config.SR_VOTING,
            # tmp fix for TypeError: Object of type SrVote is not JSON serializable
            "sr_votes": str(config.SR_VOTES),
            "sr_voting_allow_burn_trx": config.SR_VOTING_ALLOW_BURN_TRX,
        }

        return {
            "status": "success",
            "config": staking_config,
            "fee_deposit_account": {
                "address": fee_deposit_address,
                "is_active": fee_deposit_status,
                "info": fee_deposit_info,
            },
            "energy_delegator_account": {
                "address": energy_delegator_address,
                "is_active": energy_delegator_status,
                "info": energy_delegator_info,
                "is_externally_managed": energy_delegator_priv is None,
                "is_same_as_fee_deposit": energy_delegator_address
                == fee_deposit_address,
            },
        }
    except Exception as e:
        logger.exception("Error getting staking info")
        return {
            "status": "error",
            "msg": str(e),
        }


@staking_bp.get("/", defaults={"address": None})
@staking_bp.get("/<address>")
def get_resources(address):
    try:
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
    except AddressNotFound:
        return {
            "status": "error",
            "msg": "account not found on-chain",
            "details": {"address": address},
        }
    except Exception as e:
        return {
            "status": "error",
            "msg": str(e),
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


@staking_bp.post("/withdraw_stake_balance")
def withdraw_stake_balance():
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


@staking_bp.post("/delegate/<string:address>/<int:amount>/<string:res_type>")
def delegate(address: str, amount: int, res_type: Literal["ENERGY", "BANDWIDTH"]):
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    sun = int(amount * 1_000_000)
    unsigned_tx = tron_client.trx.delegate_resource(
        owner=energy_delegator_pub,
        receiver=address,
        balance=sun,
        resource=res_type,
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    tx_info = signed_tx.broadcast().wait()
    logger.info(
        f"Delegated {amount} staked TRX of {res_type} to address {address}. TXID: {unsigned_tx.txid}"
    )
    logger.info(tx_info)
    return tx_info


@staking_bp.post("/undelegate/<string:address>/<int:amount>/<string:res_type>")
def undelegate(address: str, amount: int, res_type: Literal["ENERGY", "BANDWIDTH"]):
    energy_delegator_priv, energy_delegator_pub = get_energy_delegator()
    tron_client: Tron = ConnectionManager.client()
    sun = int(amount * 1_000_000)
    unsigned_tx = tron_client.trx.undelegate_resource(
        owner=energy_delegator_pub,
        receiver=address,
        balance=sun,
        resource=res_type,
    ).build()
    signed_tx = unsigned_tx.sign(energy_delegator_priv)
    tx_info = signed_tx.broadcast().wait()
    logger.info(
        f"Undelegated {amount} staked TRX of {res_type} from address {address}. TXID: {unsigned_tx.txid}"
    )
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
