from flask import current_app
from werkzeug.exceptions import BadRequest, Conflict

from ..repositories import KeyRepository
from ..utils import get_key, init_wallet
from ..schemas import KeyType
from . import tenant_bp


@tenant_bp.post("/<int:store_id>")
def create_tenant(store_id: int):
    if store_id <= 0:
        raise BadRequest("store_id must be a positive integer")

    if KeyRepository.store_exists(store_id):
        raise Conflict(f"store_id {store_id} already exists")

    init_wallet(current_app, store_id=store_id)
    repository = KeyRepository(store_id=store_id)
    fee_deposit = repository.get_by_type(KeyType.fee_deposit)
    energy_deposit = repository.get_by_type(KeyType.energy)

    return {
        "status": "success",
        "store_id": store_id,
        "fee_deposit": fee_deposit["public"] if fee_deposit else None,
        "energy_deposit": energy_deposit["public"] if energy_deposit else None,
    }