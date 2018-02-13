CREATE TABLE IF NOT EXISTS tokens (
    address VARCHAR UNIQUE, -- contract address
    symbol VARCHAR PRIMARY KEY, -- currency symbol
    name VARCHAR, -- verbose name
    decimals INTEGER, -- currency decimal points
    icon BYTEA, -- png data
    hash VARCHAR,
    last_modified TIMESTAMP WITHOUT TIME ZONE DEFAULT (now() AT TIME ZONE 'utc')
);

CREATE INDEX IF NOT EXISTS idx_tokens_address ON tokens (address);
