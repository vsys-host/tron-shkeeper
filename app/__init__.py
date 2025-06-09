from celery import Celery
from flask import Flask

from .config import config
from . import block_scanner
from . import connection_manager
from .wallet_encryption import wallet_encryption


celery = Celery(
    __name__,
    broker=f"redis://{config.REDIS_HOST}",
    backend=f"redis://{config.REDIS_HOST}",
    task_serializer="pickle",
    accept_content=["pickle"],
    result_serializer="pickle",
    result_accept_content=["pickle"],
)

import decimal, sqlite3

sqlite3.register_adapter(decimal.Decimal, lambda x: str(x))
sqlite3.register_converter("DECTEXT", lambda x: decimal.Decimal(x.decode()))

def init_settings_table(db):
    create_table_sql = """
    CREATE TABLE IF NOT EXISTS settings (
        name TEXT PRIMARY KEY,
        value TEXT
    );
    """
    db.query_db2(create_table_sql)

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
    init_settings_table(db)
    key_type = "only_read" if app.config.READ_MODE else "onetime"

    rows = db.query_db2(f'SELECT public FROM keys WHERE type = "{key_type}"')
    accounts = [row["public"] for row in rows]

    block_scanner.BlockScanner.set_watched_accounts(accounts)

    from . import utils

    utils.init_wallet(app)

    app.url_map.converters["decimal"] = utils.DecimalConverter

    from .api import api as api_blueprint

    app.register_blueprint(api_blueprint)

    from .api import metrics_blueprint

    app.register_blueprint(metrics_blueprint)

    from .db import engine, SQLModel

    SQLModel.metadata.create_all(engine)

    return app
