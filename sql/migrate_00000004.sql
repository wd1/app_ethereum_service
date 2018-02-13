ALTER TABLE transactions RENAME COLUMN confirmed TO updated;
UPDATE transactions SET updated = created WHERE updated IS NULL;
ALTER TABLE transactions ALTER COLUMN updated SET DEFAULT (now() AT TIME ZONE 'utc');
