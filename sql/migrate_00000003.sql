ALTER TABLE transactions ADD COLUMN transaction_id BIGSERIAL;

ALTER TABLE transactions DROP CONSTRAINT transactions_pkey;
ALTER TABLE transactions ADD PRIMARY KEY (transaction_id);

ALTER TABLE transactions RENAME COLUMN transaction_hash TO hash;
ALTER TABLE transactions RENAME COLUMN last_status TO status;

ALTER TABLE transactions ADD COLUMN gas VARCHAR;
ALTER TABLE transactions ADD COLUMN gas_price VARCHAR;
ALTER TABLE transactions ADD COLUMN data VARCHAR;
ALTER TABLE transactions ADD COLUMN signature VARCHAR;

ALTER TABLE transactions ADD COLUMN blocknumber BIGINT;

ALTER TABLE transactions DROP COLUMN estimated_gas_cost;

CREATE INDEX IF NOT EXISTS idx_transactions_hash_by_id_sorted ON transactions (hash, transaction_id DESC);

ALTER TABLE notification_registrations ADD COLUMN service VARCHAR;
ALTER TABLE notification_registrations ADD COLUMN registration_id VARCHAR;

UPDATE notification_registrations nr SET service=pnr.service, registration_id=pnr.registration_id
FROM push_notification_registrations pnr
WHERE nr.token_id=pnr.token_id;

-- make sure no entries exist where service or registration_id is empty
DELETE FROM notification_registrations WHERE service IS NULL OR registration_id IS NULL;

ALTER TABLE notification_registrations DROP CONSTRAINT notification_registrations_pkey;
ALTER TABLE notification_registrations ADD PRIMARY KEY (token_id, service, registration_id, eth_address);

DROP TABLE push_notification_registrations;

CREATE INDEX IF NOT EXISTS idx_notification_registrations_eth_address ON notification_registrations (eth_address);
