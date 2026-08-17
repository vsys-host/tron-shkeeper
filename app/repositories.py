from decimal import Decimal
from typing import Any, cast

from .db import query_db2
from .logging import logger


class BalanceRepository:
    """Raw-query access to globally unique account balances."""

    def upsert(self, account: str, symbol: str, balance: Decimal) -> None:
        query_db2(
            "INSERT INTO tron_balances (account, symbol, balance) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE balance = VALUES(balance), "
            "updated_at = CURRENT_TIMESTAMP",
            (account, symbol, balance),
        )

    def zero_out(self, account: str, symbol: str) -> None:
        self.upsert(account, symbol, Decimal(0))

    def increment(self, account: str, symbol: str, amount: Decimal) -> None:
        """Add to a tracked balance, e.g. when a deposit is observed by the block scanner."""
        query_db2(
            "INSERT INTO tron_balances (account, symbol, balance) "
            "VALUES (%s, %s, %s) "
            "ON DUPLICATE KEY UPDATE balance = balance + VALUES(balance), "
            "updated_at = CURRENT_TIMESTAMP",
            (account, symbol, amount),
        )

    def get_top_trc20_balance(self) -> dict[str, Any] | None:
        """Largest positive non-TRX balance held by any onetime account."""
        row = cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT tb.account, tb.symbol, tb.balance, k.store_id "
                "FROM tron_balances tb "
                "JOIN `keys` k ON k.public = tb.account "
                "WHERE k.type = %s AND tb.symbol != %s AND tb.balance > 0 "
                "ORDER BY tb.balance DESC LIMIT 1",
                ("onetime", "TRX"),
                one=True,
            ),
        )
        if not row:
            logger.debug("get_top_trc20_balance: no TRC20 candidates found")
        return row

    def list_trx_only_balances(self) -> list[dict[str, Any]]:
        """Onetime accounts holding TRX but no positive TRC20 balance, largest first."""
        rows = cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT tb.account, tb.balance, k.store_id "
                "FROM tron_balances tb "
                "JOIN `keys` k ON k.public = tb.account "
                "WHERE k.type = %s AND tb.symbol = %s AND tb.balance > 0 "
                "AND tb.account NOT IN ("
                "SELECT account FROM tron_balances WHERE symbol != %s AND balance > 0"
                ") "
                "ORDER BY tb.balance DESC",
                ("onetime", "TRX", "TRX"),
            )
            or [],
        )
        if not rows:
            logger.debug("list_trx_only_balances: no TRX-only candidates found")
        return rows


class KeyRepository:
    """Store-scoped access to keys owned by one store."""

    def __init__(self, store_id: int):
        if isinstance(store_id, bool) or not isinstance(store_id, int) or store_id <= 0:
            raise ValueError("store_id must be a positive integer")
        self.store_id = store_id

    def get_fee_deposit_key(self) -> dict[str, Any] | None:
        return cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT * FROM `keys` WHERE store_id = %s AND type = %s LIMIT 1",
                (self.store_id, "fee_deposit"),
                one=True,
            ),
        )

    @staticmethod
    def store_exists(store_id: int) -> bool:
        row = cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT 1 FROM `keys` WHERE store_id = %s LIMIT 1",
                (store_id,),
                one=True,
            ),
        )
        return row is not None

    def list_onetime_addresses(self) -> list[str]:
        rows = cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT public FROM `keys` WHERE store_id = %s AND type = %s",
                (self.store_id, "onetime"),
            )
            or [],
        )
        return [row["public"] for row in rows]

    def list_onetime_keys(self) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT public, symbol FROM `keys` "
                "WHERE store_id = %s AND type = %s",
                (self.store_id, "onetime"),
            )
            or [],
        )

    def get_by_type(self, key_type: str) -> dict[str, Any] | None:
        return cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT * FROM `keys` WHERE store_id = %s AND type = %s LIMIT 1",
                (self.store_id, key_type),
                one=True,
            ),
        )

    def list_for_dump(self, symbol: str) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT * FROM `keys` "
                "WHERE store_id = %s AND (symbol = %s OR type != %s)",
                (self.store_id, symbol, "one_time"),
            )
            or [],
        )

    def list_addresses_for_symbol(self, symbol: str) -> list[str]:
        rows = cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT public FROM `keys` "
                "WHERE store_id = %s AND (symbol = %s OR type = %s)",
                (self.store_id, symbol, "fee_deposit"),
            )
            or [],
        )
        return [row["public"] for row in rows]

    def get_by_public(self, public: str) -> dict[str, Any] | None:
        return cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT * FROM `keys` WHERE store_id = %s AND public = %s LIMIT 1",
                (self.store_id, public),
                one=True,
            ),
        )

    def get_by_public_and_type(
        self, public: str, key_type: str
    ) -> dict[str, Any] | None:
        return cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT * FROM `keys` "
                "WHERE store_id = %s AND public = %s AND type = %s LIMIT 1",
                (self.store_id, public, key_type),
                one=True,
            ),
        )

    def create_onetime_key(self, symbol: str, public: str, private: str) -> None:
        query_db2(
            "INSERT INTO `keys` (store_id, symbol, public, private, type) "
            "VALUES (%s, %s, %s, %s, %s)",
            (self.store_id, symbol, public, private, "onetime"),
        )

    def create_external_key(self, key_type: str, public: str, private: str) -> None:
        query_db2(
            "INSERT INTO `keys` (store_id, symbol, public, private, type) "
            "VALUES (%s, %s, %s, %s, %s)",
            (self.store_id, "_", public, private, key_type),
        )

    def update_private_key(self, public: str, private: str) -> None:
        query_db2(
            "UPDATE `keys` SET private = %s "
            "WHERE store_id = %s AND public = %s",
            (private, self.store_id, public),
        )


class AllStoresKeyReader:
    """Read key addresses across all stores for block scanning."""

    def list_watched_addresses(self) -> list[str]:
        rows = cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT public FROM `keys` "
                "WHERE type IN (%s, %s)",
                ("onetime", "fee_deposit"),
            )
            or [],
        )
        return [row["public"] for row in rows]

    def list_onetime_accounts(self) -> list[dict[str, Any]]:
        return cast(
            list[dict[str, Any]],
            query_db2(
                "SELECT public, store_id FROM `keys` WHERE type = %s",
                ("onetime",),
            )
            or [],
        )

    def get_store_id_by_public(self, public: str) -> int | None:
        row = cast(
            dict[str, Any] | None,
            query_db2(
                "SELECT store_id FROM `keys` WHERE public = %s LIMIT 1",
                (public,),
                one=True,
            ),
        )
        return int(row["store_id"]) if row else None
