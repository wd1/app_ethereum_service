CREATE TABLE IF NOT EXISTS filter_registrations (
    filter_id VARCHAR,
    registration_id VARCHAR,
    contract_address VARCHAR,
    topic_id VARCHAR,
    topic VARCHAR,

    PRIMARY KEY (filter_id),
    UNIQUE (registration_id, contract_address, topic_id)
);

CREATE INDEX IF NOT EXISTS idx_filter_registrations_contract_address_topic ON filter_registrations (contract_address, topic_id);
CREATE INDEX IF NOT EXISTS idx_filter_registrations_filter_id_registration_id ON filter_registrations (filter_id, registration_id);
