ALTER TABLE transactions ADD COLUMN nonce BIGINT;
UPDATE transactions SET nonce = -1;
ALTER TABLE transactions ALTER COLUMN nonce SET NOT NULL;
ALTER TABLE transactions ADD COLUMN error INTEGER;
ALTER TABLE transactions ADD COLUMN last_status VARCHAR;

CREATE INDEX IF NOT EXISTS idx_transactions_from_address_nonce ON transactions (from_address, nonce DESC);
