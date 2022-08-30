import traceback
from flask import Blueprint, g, request
from werkzeug.exceptions import HTTPException

from ..config import config
from ..logging import logger

api = Blueprint('api', __name__, url_prefix='/<symbol>')

@api.before_request
def check_credentials():
    auth = request.authorization
    if not (auth and auth.username == config['API_USERNAME']
                 and auth.password == config['API_PASSWORD']):
            return {'status': 'error', 'msg': 'authorization requred'}, 401

@api.url_defaults
def add_symbol(endpoint, values):
    values.setdefault('symbol', g.symbol)

@api.url_value_preprocessor
def pull_symbol(endpoint, values):
    g.symbol = values.pop('symbol').upper()


@api.errorhandler(Exception)
def handle_exception(e):
    if isinstance(e, HTTPException):
        return e
    logger.warn(f"Exception: {traceback.format_exc()}")
    return {"status": "error", "msg": str(e)}

from . import payout, views
