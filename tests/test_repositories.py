from typing import Any

import pytest

from app import repositories


def test_key_repository_requires_positive_store_id():
    invalid_store_ids: tuple[Any, ...] = (None, 0, -1, "1", True)
    for store_id in invalid_store_ids:
        with pytest.raises(ValueError):
            repositories.KeyRepository(store_id)


def test_key_repository_reads_are_scoped_to_store(monkeypatch):
    calls = []

    def fake_query_db2(query, args=(), one=False):
        calls.append((query, args, one))
        if "SELECT public" in query:
            return [{"public": "TOnetime"}]
        return {"public": "TFee", "store_id": 7}

    monkeypatch.setattr(repositories, "query_db2", fake_query_db2)
    key_repository = repositories.KeyRepository(store_id=7)

    fee_deposit_key = key_repository.get_fee_deposit_key()
    assert fee_deposit_key is not None
    assert fee_deposit_key["public"] == "TFee"
    assert key_repository.list_onetime_addresses() == ["TOnetime"]
    address_key = key_repository.get_by_public("TAddress")
    assert address_key is not None
    assert address_key["store_id"] == 7

    assert len(calls) == 3
    assert all("store_id = %s" in query for query, _, _ in calls)
    assert calls[0][1] == (7, "fee_deposit")
    assert calls[1][1] == (7, "onetime")
    assert calls[2][1] == (7, "TAddress")


def test_key_repository_writes_store_id(monkeypatch):
    calls = []

    def fake_query_db2(query, args=(), one=False):
        calls.append((query, args, one))

    monkeypatch.setattr(repositories, "query_db2", fake_query_db2)
    key_repository = repositories.KeyRepository(store_id=7)

    key_repository.create_onetime_key("TRX", "TOne", "private")
    key_repository.create_external_key("fee_deposit", "TFee", "EXTERNALLY_MANAGED")
    key_repository.update_private_key("TFee", "encrypted")

    assert calls[0][1] == (7, "TRX", "TOne", "private", "onetime")
    assert calls[1][1] == (7, "_", "TFee", "EXTERNALLY_MANAGED", "fee_deposit")
    assert calls[2][1] == ("encrypted", 7, "TFee")
    assert "WHERE store_id = %s" in calls[2][0]


def test_all_stores_key_reader_lists_onetime_and_fee_deposit_addresses(monkeypatch):
    calls = []

    def fake_query_db2(query, args=(), one=False):
        calls.append((query, args, one))
        return [{"public": "TOne"}, {"public": "TFee"}]

    monkeypatch.setattr(repositories, "query_db2", fake_query_db2)

    assert repositories.AllStoresKeyReader().list_watched_addresses() == [
        "TOne",
        "TFee",
    ]
    assert calls[0][1] == ("onetime", "fee_deposit")
    assert "store_id" not in calls[0][0]


def test_all_stores_key_reader_resolves_store_id(monkeypatch):
    calls = []

    def fake_query_db2(query, args=(), one=False):
        calls.append((query, args, one))
        return {"store_id": "7"}

    monkeypatch.setattr(repositories, "query_db2", fake_query_db2)

    assert repositories.AllStoresKeyReader().get_store_id_by_public("TOne") == 7
    assert calls[0][1] == ("TOne",)
    assert calls[0][2] is True