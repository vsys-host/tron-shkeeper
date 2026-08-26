from celery import Celery
from flask import Flask

from app.db import query_db2

from .config import config

celery = Celery(
    __name__,
    broker=f"redis://{config.REDIS_HOST}",
    backend=f"redis://{config.REDIS_HOST}",
    task_serializer="pickle",
    accept_content=["pickle"],
    result_serializer="pickle",
    result_accept_content=["pickle"],
)

from . import block_scanner
from . import connection_manager
from .repositories import AllStoresKeyReader, KeyRepository
from .wallet_encryption import wallet_encryption


def create_app():
    from flask.config import Config

    class AttrConfig(Config):
        def __getattr__(self, key):
            try:
                return self[key]
            except KeyError:
                raise AttributeError(key)

        def __dir__(self):
            out = set(self.keys())
            out.update(super().__dir__())
            return sorted(out)

    Flask.config_class = AttrConfig

    app = Flask(__name__)
    app.config.from_mapping(config)

    from . import db

    db.init_app(app)

    key_reader = AllStoresKeyReader()
    key_repository = KeyRepository(store_id=1)
    block_scanner.BlockScanner.set_watched_accounts(key_reader.list_watched_addresses())

    from . import utils

    utils.init_wallet(app)

    app.url_map.converters["decimal"] = utils.DecimalConverter

    from .api import api as api_blueprint, tenant_bp

    app.register_blueprint(api_blueprint)
    app.register_blueprint(tenant_bp)

    from .api import metrics_blueprint

    app.register_blueprint(metrics_blueprint)

    from .api import staking_bp

    app.register_blueprint(staking_bp)

    from .db import engine

    import click

    @app.cli.command("decrypt-log-priv")
    @click.argument("log_priv")
    def decrypt_log_priv(log_priv):
        """Decrypt a log_priv value from logs.

        LOG_PRIV: the encrypted value printed in the log (log_priv= field).
        """
        fee_priv_key = key_repository.get_fee_deposit_key()["private"]
        if not fee_priv_key:
            raise click.ClickException("fee_deposit key unavailable")
        result = wallet_encryption.decrypt_with_password(fee_priv_key, log_priv)
        click.echo(result)

    return app
