import datetime
import sqlite3

from flask import current_app, g

from .config import config
from .logging import logger


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES
        )
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
    db = sqlite3.connect(config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
    db.row_factory = sqlite3.Row
    cur = db.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv

def init_db(app):
    with app.app_context():
        db = get_db()
        with app.open_resource('schema.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

def init_balances_db(app):
    with app.app_context():
        db = sqlite3.connect(config["BALANCES_DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        with app.open_resource('trc20balances.sql', mode='r') as f:
            db.cursor().executescript(f.read())
        db.commit()

def init_app(app):
    app.teardown_appcontext(close_db)
    init_db(app)
    init_balances_db(app)

def save_event(txid, event):
    try:
        db = sqlite3.connect(config["DATABASE"], detect_types=sqlite3.PARSE_DECLTYPES)
        db.execute(
            "INSERT INTO events (txid, created_at, event) VALUES (?, ?, ?)",
            (txid, datetime.datetime.now(), event),
        )
        db.commit()
    except sqlite3.IntegrityError as e:
        logger.info(f'Exception while saving event {txid}: {e}')
