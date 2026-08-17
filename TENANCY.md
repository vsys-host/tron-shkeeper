# Tenancy Status

## Tenancy-ready

- **Key repository:** `KeyRepository` requires a positive `store_id`. Tenant-scoped reads and writes include `store_id` in their SQL predicates.
- **API requests:** `X-Store-ID` selects the request store. Missing markers default to store `1` for legacy callers; invalid markers are rejected.
- **API key operations:** address generation, key listing, dumps, and fee-deposit lookups use the request store.
- **Wallet and staking:** wallet construction and staking key lookups receive the request `store_id`.
- **Payout tasks:** payout preparation, execution, and result notifications propagate `store_id`. Existing task callers remain compatible through a default of `1`.
- **Sweep tasks:** TRX/TRC20 transfers and energy undelegation receive the owning store ID.
- **Block scanner:** watched-address discovery is global, but each detected address is resolved to its owner before a sweep task is dispatched. Stale addresses are not swept.
- **Balance scan account ownership:** `balance_collector` discovers onetime accounts across stores and carries each account's owner ID into `tron_balances`; `funds_sweeper` reads it back via `keys.store_id` for sweep calls.
- **Public address ownership:** `keys.public` is globally unique, allowing deterministic address-to-store lookup.

## Not tenancy-ready

- Legacy callers that omit `store_id` still operate against store `1`; they are compatible but not dynamically tenant-aware.
- Some system initialization and utility paths intentionally construct a store-1 repository because the current deployment has one active store.
- Periodic SR voting is explicitly hard-coded to store `1`.
- Global wallet-encryption and validation routines still operate across the complete `keys` table.

## Intentionally global or out of scope

- `settings` values such as block-scanner position and current server selection are system-wide.
- Block-scanner watched-address discovery is global by design; tenant ownership is applied only when dispatching tenant-specific work.
- SQLModel-managed persistence remains outside this raw-key repository boundary.
- Legacy remote task payloads and endpoint shapes are preserved; tenancy is added through optional trailing parameters and the optional request marker.
