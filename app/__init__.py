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

    block_scanner.BlockScanner.set_watched_accounts(
        [
            row["public"]
            for row in db.query_db2('select public from keys where type = "onetime"')
        ]
    )

    from . import utils

    utils.init_wallet(app)

    # add fee-deposit account to watch list
    block_scanner.BlockScanner.add_watched_account(
        db.query_db2('select * from keys where type = "fee_deposit" ', one=True)[
            "public"
        ]
    )

    app.url_map.converters["decimal"] = utils.DecimalConverter

    from .api import api as api_blueprint

    app.register_blueprint(api_blueprint)

    from .api import metrics_blueprint

    app.register_blueprint(metrics_blueprint)

    from .api import staking_bp

    app.register_blueprint(staking_bp)

    from .db import engine, SQLModel

    SQLModel.metadata.create_all(engine)

    return app
