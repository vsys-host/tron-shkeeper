from decimal import Decimal

import pytest
from flask import Flask

from app.api import tenant_bp
from app.api import tenant as tenant_api
from app.repositories import BalanceRepository, KeyRepository


@pytest.fixture
def tenant_app():
    app = Flask(__name__)
    app.config["TESTING"] = True
    app.register_blueprint(tenant_bp)
    return app
    return app


def auth_headers():
    return {"Authorization": "Basic c2hrZWVwZXI6c2hrZWVwZXI="}


def test_create_tenant_uses_path_store_id(monkeypatch, tenant_app):
    provisioned = {}
    keys = {
        "fee_deposit": {"public": "fee-address"},
        "energy_delegation": {"public": "energy-address"},
    }

    monkeypatch.setattr(
        tenant_api,
        "init_wallet",
        lambda app, store_id: provisioned.update(store_id=store_id),
    )

    class Repository:
        @staticmethod
        def store_exists(store_id):
            return False

        def __init__(self, store_id):
            pass

        def get_by_type(self, key_type):
            return keys.get(key_type)

    monkeypatch.setattr(
        tenant_api,
        "KeyRepository",
        Repository,
    )

    response = tenant_app.test_client().post(
        "/tenant/7",
        headers={**auth_headers(), "X-Store-ID": "99"},
    )

    assert response.status_code == 200
    assert provisioned == {"store_id": 7}
    assert response.get_json() == {
        "status": "success",
        "store_id": 7,
        "fee_deposit": "fee-address",
        "energy_deposit": "energy-address",
    }


def test_create_tenant_rejects_existing_store(monkeypatch, tenant_app):
    class Repository:
        @staticmethod
        def store_exists(store_id):
            return True

    monkeypatch.setattr(tenant_api, "KeyRepository", Repository)
    response = tenant_app.test_client().post("/tenant/7", headers=auth_headers())

    assert response.status_code == 409
    assert "already exists" in response.get_data(as_text=True)


def test_create_tenant_requires_auth(tenant_app):
    response = tenant_app.test_client().post("/tenant/7")

    assert response.status_code == 401


def test_balance_repository_upserts_decimal(monkeypatch):
    calls = []
    monkeypatch.setattr(
        "app.repositories.query_db2",
        lambda query, args=(), one=False: calls.append((query, args, one)),
    )

    BalanceRepository().upsert("account", "TRX", Decimal("1.25"))

    assert calls == [
        (
            "INSERT INTO tron_balances (account, symbol, balance) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE balance = VALUES(balance), "
            "updated_at = CURRENT_TIMESTAMP",
            ("account", "TRX", Decimal("1.25")),
            False,
        )
    ]
