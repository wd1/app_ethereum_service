ALTER TABLE transactions ADD COLUMN v VARCHAR;
ALTER TABLE transactions ADD COLUMN r VARCHAR;
ALTER TABLE transactions ADD COLUMN s VARCHAR;

UPDATE transactions SET v = CASE WHEN substring(signature, 132) = '1' THEN '0x1c' ELSE '0x1b' END, r = substring(signature, 0, 67), s = '0x' || substring(signature, 67, 64) WHERE signature IS NOT NULL;

ALTER TABLE transactions DROP COLUMN signature;
