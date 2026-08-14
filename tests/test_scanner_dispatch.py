from types import SimpleNamespace
from unittest.mock import MagicMock

import pytest

from app import block_scanner, tasks
from app.schemas import TronSymbol


def make_transaction(symbol, destination, is_trc20):
    return SimpleNamespace(
        symbol=symbol,
        src_addr="source",
        dst_addr=destination,
        txid="txid",
        status="SUCCESS",
        is_trc20=is_trc20,
    )


def configure_scan(monkeypatch, transaction, owner_id):
    destination = transaction.dst_addr
    scanner = block_scanner.BlockScanner()
    reader = MagicMock()
    reader.get_store_id_by_public.return_value = owner_id

    scanner.download_block = lambda block_num: {"transactions": [{"txID": "txid"}]}
    scanner.download_tx_info_by_block_num = lambda block_num: {}
    scanner.notify_shkeeper = MagicMock()
    monkeypatch.setattr(block_scanner, "parse_tx", lambda tx, info: [transaction])
    monkeypatch.setattr(block_scanner, "AllStoresKeyReader", lambda: reader)
    monkeypatch.setattr(
        block_scanner.BlockScanner,
        "get_watched_accounts",
        classmethod(lambda cls: {destination}),
    )
    return scanner, reader


def test_scanner_dispatches_trc20_with_owner_in_dev_mode(monkeypatch):
    transaction = make_transaction(TronSymbol.USDT, "watched", True)
    scanner, reader = configure_scan(monkeypatch, transaction, 7)
    transfer = MagicMock()
    monkeypatch.setattr(tasks, "transfer_trc20_from", transfer)
    monkeypatch.setattr(block_scanner.config, "DEVMODE_CELERY_NODELAY", True)

    assert scanner.scan(1) is True
    reader.get_store_id_by_public.assert_called_once_with("watched")
    transfer.assert_called_once_with("watched", TronSymbol.USDT, 7)
    scanner.notify_shkeeper.assert_called_once_with("USDT", "txid")


def test_scanner_dispatches_trx_asynchronously_with_owner(monkeypatch):
    transaction = make_transaction(TronSymbol.TRX, "watched", False)
    scanner, reader = configure_scan(monkeypatch, transaction, 7)
    transfer = MagicMock()
    transfer.delay = MagicMock()
    monkeypatch.setattr(tasks, "transfer_trx_from", transfer)
    monkeypatch.setattr(block_scanner.config, "ENERGY_DELEGATION_MODE", False)

    assert scanner.scan(1) is True
    reader.get_store_id_by_public.assert_called_once_with("watched")
    transfer.delay.assert_called_once_with("watched", 7)


def test_scanner_skips_sweep_for_stale_watched_address(monkeypatch):
    transaction = make_transaction(TronSymbol.USDT, "stale", True)
    scanner, reader = configure_scan(monkeypatch, transaction, None)
    transfer = MagicMock()
    transfer.delay = MagicMock()
    warning = MagicMock()
    monkeypatch.setattr(block_scanner.logger, "warning", warning)
    monkeypatch.setattr(tasks, "transfer_trc20_from", transfer)

    assert scanner.scan(1) is True
    reader.get_store_id_by_public.assert_called_once_with("stale")
    transfer.delay.assert_not_called()
    assert "without an owner" in warning.call_args.args[0]


def test_payout_propagates_store_id_to_result_notification(monkeypatch):
    wallet = MagicMock()
    wallet.transfer.return_value = {"status": "success"}
    notification = MagicMock()
    notification.delay = MagicMock()
    monkeypatch.setattr(tasks, "Wallet", lambda *args, **kwargs: wallet)
    monkeypatch.setattr(tasks, "post_payout_results", notification)

    result = tasks.payout.run(
        [{"dst": "destination", "amount": 1}], "TRX", store_id=7
    )

    assert result == [{"status": "success"}]
    notification.delay.assert_called_once_with(
        [{"status": "success"}], "TRX", 7
    )