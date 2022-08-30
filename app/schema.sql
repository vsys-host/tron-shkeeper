CREATE TABLE IF NOT EXISTS keys (
  id INTEGER PRIMARY KEY AUTOINCREMENT,
  public TEXT NOT NULL,
  private TEXT NOT NULL,
  symbol TEXT NOT NULL,
  type TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS events (
  txid TEXT PRIMARY KEY,
  created_at timestamp,
  event TEXT
);

