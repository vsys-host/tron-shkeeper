CREATE TABLE IF NOT EXISTS trc20balances (
  account TEXT NOT NULL,
  symbol TEXT NOT NULL,
  balance DECTEXT NOT NULL,
  updated_at timestamp,

  UNIQUE(account, symbol)
);