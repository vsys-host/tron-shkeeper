import traceback
from flask import Blueprint, g, request
from werkzeug.exceptions import BadRequest, HTTPException

from ..config import config
from ..logging import logger

api = Blueprint("api", __name__, url_prefix="/<symbol>")
tenant_bp = Blueprint("tenant_bp", __name__, url_prefix="/tenant")
metrics_blueprint = Blueprint("metrics_blueprint", __name__, url_prefix="/")
staking_bp = Blueprint("staking_bp", __name__, url_prefix="/staking")
DEFAULT_STORE_ID = 1


def resolve_store_id() -> int:
    marker = request.headers.get("X-Store-ID")
    if marker is None:
        return DEFAULT_STORE_ID

    try:
        store_id = int(marker)
    except ValueError as exc:
        raise BadRequest("X-Store-ID must be a positive integer") from exc

    if store_id <= 0:
        raise BadRequest("X-Store-ID must be a positive integer")
    return store_id


@staking_bp.before_request
@metrics_blueprint.before_request
@api.before_request
def check_credentials():
    response = authenticate()
    if response:
        return response
    g.store_id = resolve_store_id()


@tenant_bp.before_request
def check_tenant_credentials():
    return authenticate()


def authenticate():
    auth = request.authorization
    if not (
        auth
        and auth.username == config.API_USERNAME
        and auth.password == config.API_PASSWORD
    ):
        return {"status": "error", "msg": "authorization requred"}, 401


@api.url_defaults
def add_symbol(endpoint, values):
    values.setdefault("symbol", g.symbol)


@api.url_value_preprocessor
def pull_symbol(endpoint, values):
    g.symbol = values.pop("symbol").upper()


@api.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return e
    logger.warn(f"Exception: {traceback.format_exc()}")
    return {"status": "error", "msg": str(e)}


from . import payout, tenant, views, metrics, staking
