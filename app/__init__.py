from celery import Celery
from flask import Flask

from . import events
from .config import config

celery = Celery(
    __name__,
    broker=f'redis://{config["REDIS_HOST"]}',
    backend=f'redis://{config["REDIS_HOST"]}',
    task_serializer='pickle',
    accept_content=['pickle'],
    result_serializer='pickle',
    result_accept_content=['pickle'],
)

def create_app():

    app = Flask(__name__)
    app.config.from_mapping(config)

    from . import db
    db.init_app(app)

    from . import utils
    utils.init_wallet(app)

    app.url_map.converters['decimal'] = utils.DecimalConverter

    with app.app_context():
        events.FILTER = utils.get_filter_config()

    from .api import api as api_blueprint
    app.register_blueprint(api_blueprint)

    return app
