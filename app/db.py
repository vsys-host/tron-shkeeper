import time

import pymysql
import pymysql.cursors
from flask import g
from sqlalchemy import NullPool, create_engine

from .config import config

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
    # autocommit=True replicates the previous sqlite3 isolation_level=None
    # (autocommit) behavior for the raw query_db/query_db2 layer below.
    #
    # NOTE: cursorclass is deliberately NOT overridden here. SQLAlchemy's Core/
    # Raw query helpers request DictCursor explicitly per cursor.
    connect_args={"autocommit": True},
    # echo=True,
)


def get_db():
    if "db" not in g:
        g.db = engine.raw_connection()

    return g.db


def close_db(e=None):
    db = g.pop("db", None)
    if db is not None:
        db.close()


def query_db(query, args=(), one=False):
    cur = get_db().cursor(pymysql.cursors.DictCursor)
    cur.execute(query, args)
    rv = cur.fetchall()
    cur.close()
    return (rv[0] if rv else None) if one else rv


def query_db2(query, args=(), one=False):
    start_time = time.time()
    conn = engine.raw_connection()
    try:
        cur = conn.cursor(pymysql.cursors.DictCursor)
        cur.execute(query, args)
        rv = cur.fetchall()
        cur.close()
    finally:
        conn.close()
    # logger.debug(f'query_db2({query}) took {time.time() - start_time} seconds')
    return (rv[0] if rv else None) if one else rv


def init_db(app):
    with app.open_resource("schema.sql", mode="r") as f:
        statements = [s.strip() for s in f.read().split(";") if s.strip()]
    conn = engine.raw_connection()
    try:
        cur = conn.cursor()
        for statement in statements:
            cur.execute(statement)
        cur.close()
        conn.commit()
    finally:
        conn.close()


def init_app(app):
    app.teardown_appcontext(close_db)
    init_db(app)
