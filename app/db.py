import sqlite3
import time

from flask import current_app, g
from sqlalchemy import NullPool
from sqlmodel import SQLModel, create_engine  # noqa: F401

from .config import config
from . import models
from .custom.aml import models  # noqa: F401, F811

engine = create_engine(
    config.DB_URI,
    # https://stackoverflow.com/a/73764136
    # Sqlalchemy pools connections by default in a non-threadsafe manner,
    # Celery forks processes by default: one or the other needs to be changed.
    # Solution 1) Turn off Sqlalchemy pooling
    # (we ended up going with this to maintain better concurrency)
    #
    # from sqlalchemy.pool import NullPool
    # engine = create_engine(
    #     SQLALCHEMY_DATABASE_URL, poolclass=NullPool
    # )
    # Solution 2) Make Celery run as a single process with no forking
    # meant that all of our celery tasks would be running serially,
    # which eliminated the errors but we needed more bandwidth.
    # This may be fine for certain applications.
    #
    # celery -A celery_worker.celery worker -E --loglevel=info --pool=solo
    poolclass=NullPool,
    # echo=True,
)


def get_db():
    if "db" not in g:
        g.db = sqlite3.connect(
            current_app.config.DATABASE,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
        )
        g.db.execute("pragma journal_mode=wal;")
        g.db.row_factory = sqlite3.Row

    return g.db


def get_db2():
    db = sqlite3.connect(
        config.DATABASE,
        detect_types=sqlite3.PARSE_DECLTYPES,
        isolation_level=None,
    )
    db.execute("pragma journal_mode=wal;")
    db.row_factory = sqlite3.Row

    return db


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
    db = sqlite3.connect(
        config.DATABASE, detect_types=sqlite3.PARSE_DECLTYPES, isolation_level=None
    )
    db.execute("pragma journal_mode=wal;")
    db.row_factory = sqlite3.Row
    cur = db.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    # logger.debug(f'query_db2({query}) took {time.time() - start_time} seconds')
    return (rv[0] if rv else None) if one else rv


def init_db(app):
    with app.app_context():
        db = get_db()
        with app.open_resource("schema.sql", mode="r") as f:
            db.cursor().executescript(f.read())
        db.commit()


def init_balances_db(app):
    with app.app_context():
        db = sqlite3.connect(
            config.BALANCES_DATABASE,
            detect_types=sqlite3.PARSE_DECLTYPES,
            isolation_level=None,
        )
        db.execute("pragma journal_mode=wal;")
        with app.open_resource("trc20balances.sql", mode="r") as f:
            db.cursor().executescript(f.read())
        db.commit()


def init_app(app):
    app.teardown_appcontext(close_db)
    init_db(app)
    init_balances_db(app)
