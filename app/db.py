import datetime
import sqlite3
import time

from flask import current_app, g

from .config import config
from .logging import logger


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None
        )
        g.db.execute('pragma journal_mode=wal;')
        g.db.row_factory = sqlite3.Row

    return g.db

def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()

def query_db(query, args=(), one=False):
    cur = get_db().execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def query_db2(query, args=(), one=False):
    start_time = time.time()
    db = sqlite3.connect(config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
    db.execute('pragma journal_mode=wal;')
    db.row_factory = sqlite3.Row
    cur = db.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    logger.debug(f'query_db2({query}) took {time.time() - start_time} seconds')
    return (rv[0] if rv else None) if one else rv

def init_db(app):
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

def init_balances_db(app):
    with app.app_context():
        db = sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None)
        db.execute('pragma journal_mode=wal;')
        with app.open_resource('trc20balances.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

def init_app(app):
    app.teardown_appcontext(close_db)
    init_db(app)
    init_balances_db(app)
