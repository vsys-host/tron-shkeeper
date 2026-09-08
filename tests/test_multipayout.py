from decimal import Decimal
from unittest.mock import MagicMock

import pytest
from flask import Flask

import app.api as api_module
import app.api.payout as payout_module
from app.config import config
from app.utils import DecimalConverter

# valid, arbitrary base58check Tron address used across tests
DEST = "TBkZMNASLXkRzCPfzLG6w844kvSdvo471E"


def make_wallet_class(balances):
    """Build a fake Wallet class returning preset balances keyed by symbol."""

    class FakeWallet:
        def __init__(self, symbol="TRX", store_id: int = 1):
            self.symbol = symbol
            self.store_id = store_id
            self.main_account = {"public": f"fee-deposit-{store_id}"}
            self.balance = balances[symbol]

    return FakeWallet


class FakeSignature:
    def __init__(self, task_name, args, kwargs):
        self.task_name = task_name
        self.args = args
        self.kwargs = kwargs
        self.next = None

    def __or__(self, other):
        self.next = other
        return self

    def apply_async(self):
        return MagicMock(id="task-123")


def make_task_mock(name):
    task = MagicMock()
    task.s.side_effect = lambda *a, **k: FakeSignature(name, a, k)
    return task


@pytest.fixture
def client(monkeypatch):
    monkeypatch.setattr(api_module, "authenticate", lambda: None)

    flask_app = Flask(__name__)
    flask_app.url_map.converters["decimal"] = DecimalConverter
    flask_app.register_blueprint(api_module.api)

    return flask_app.test_client()


def payload(amounts, dest=DEST):
    return [{"dest": dest, "amount": str(a)} for a in amounts]


# --- validation errors (symbol-agnostic) ---


def test_empty_payout_list_is_rejected(client):
    response = client.post("/TRX/multipayout", json=[])
    assert "empty" in response.get_json()["msg"]


def test_bad_json_is_rejected(client):
    response = client.post(
        "/TRX/multipayout", data="not json", content_type="application/json"
    )
    assert "Bad JSON" in response.get_json()["msg"]


def test_bad_destination_address_is_rejected(client):
    response = client.post("/TRX/multipayout", json=payload([1], dest="not-an-address"))
    assert "Bad destination address" in response.get_json()["msg"]


def test_bad_amount_is_rejected(client):
    response = client.post(
        "/TRX/multipayout", json=[{"dest": DEST, "amount": "not-a-number"}]
    )
    assert "Bad amount" in response.get_json()["msg"]


@pytest.mark.parametrize("amount", ["0", "-1"])
def test_non_positive_amount_is_rejected(client, amount):
    response = client.post("/TRX/multipayout", json=[{"dest": DEST, "amount": amount}])
    assert "positive number" in response.get_json()["msg"]


# --- TRX branch (native currency) ---


def test_trx_sufficient_balance_dryrun_reports_need_and_have(client, monkeypatch):
    need_currency = config.TRX_PAYOUT_FEE
    balance = Decimal(10) + need_currency
    wallet_cls = make_wallet_class({"TRX": balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/TRX/multipayout?dryrun=1", json=payload([Decimal(10)]))
    body = response.get_json()

    assert body["tokens"] == {"need": str(Decimal(10)), "have": str(balance)}
    assert body["currency"] == {"need": str(need_currency), "have": str(balance)}


def test_trx_insufficient_balance_raises(client, monkeypatch):
    need_total = Decimal(10) + config.TRX_PAYOUT_FEE
    balance = need_total - 1
    wallet_cls = make_wallet_class({"TRX": balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/TRX/multipayout?dryrun=1", json=payload([Decimal(10)]))
    msg = response.get_json()["msg"]

    assert "Not enough TRX" in msg
    assert f"Has: {balance}" in msg
    assert f"need: {need_total}" in msg


def test_trx_balance_exactly_equal_to_need_passes(client, monkeypatch):
    need_total = Decimal(10) + config.TRX_PAYOUT_FEE
    wallet_cls = make_wallet_class({"TRX": need_total})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/TRX/multipayout?dryrun=1", json=payload([Decimal(10)]))
    assert "msg" not in response.get_json()


def test_trx_fee_scales_with_number_of_transfers(client, monkeypatch):
    # 3 transfers of 5 each: need_tokens=15, fee=3*TRX_PAYOUT_FEE, total=15+3*fee
    need_total = Decimal(15) + 3 * config.TRX_PAYOUT_FEE
    balance = need_total - 1
    wallet_cls = make_wallet_class({"TRX": balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post(
        "/TRX/multipayout?dryrun=1", json=payload([Decimal(5)] * 3)
    )
    msg = response.get_json()["msg"]
    assert f"need: {need_total}" in msg


def test_trx_multipayout_dispatches_task(client, monkeypatch):
    balance = Decimal(10) + config.TRX_PAYOUT_FEE
    wallet_cls = make_wallet_class({"TRX": balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)
    monkeypatch.setattr(
        payout_module, "prepare_multipayout", make_task_mock("prepare_multipayout")
    )
    monkeypatch.setattr(payout_module, "payout_task", make_task_mock("payout"))

    response = client.post("/TRX/multipayout", json=payload([Decimal(10)]))
    assert response.get_json() == {"task_id": "task-123"}


# --- token branch (TRC20) ---


def test_token_sufficient_balances_dryrun_reports_need_and_have(client, monkeypatch):
    trx_balance = config.TX_FEE + 1
    wallet_cls = make_wallet_class({"USDT": Decimal(10), "TRX": trx_balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/USDT/multipayout?dryrun=1", json=payload([Decimal(10)]))
    body = response.get_json()

    assert body["tokens"] == {"need": str(Decimal(10)), "have": str(Decimal(10))}
    assert body["currency"] == {"need": str(config.TX_FEE), "have": str(trx_balance)}


def test_token_insufficient_token_balance_raises(client, monkeypatch):
    # regression test: this used to be a silent no-op
    trx_balance = config.TX_FEE + 1
    wallet_cls = make_wallet_class({"USDT": Decimal(5), "TRX": trx_balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/USDT/multipayout?dryrun=1", json=payload([Decimal(10)]))
    msg = response.get_json()["msg"]

    assert "Not enough USDT tokens" in msg
    assert "Has: 5" in msg
    assert "need: 10" in msg


def test_token_insufficient_trx_fee_balance_raises(client, monkeypatch):
    trx_balance = config.TX_FEE - 1 if config.TX_FEE > 0 else Decimal(-1)
    wallet_cls = make_wallet_class({"USDT": Decimal(10), "TRX": trx_balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)

    response = client.post("/USDT/multipayout?dryrun=1", json=payload([Decimal(10)]))
    msg = response.get_json()["msg"]

    assert "Not enough TRX tokens at fee-deposit account" in msg
    assert f"need: {config.TX_FEE}" in msg


def test_token_multipayout_dispatches_task(client, monkeypatch):
    trx_balance = config.TX_FEE + 1
    wallet_cls = make_wallet_class({"USDT": Decimal(10), "TRX": trx_balance})
    monkeypatch.setattr(payout_module, "Wallet", wallet_cls)
    monkeypatch.setattr(
        payout_module, "prepare_multipayout", make_task_mock("prepare_multipayout")
    )
    monkeypatch.setattr(payout_module, "payout_task", make_task_mock("payout"))

    response = client.post("/USDT/multipayout", json=payload([Decimal(10)]))
    assert response.get_json() == {"task_id": "task-123"}
