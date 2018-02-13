CREATE TABLE IF NOT EXISTS push_notification_registrations (
    service VARCHAR,
    registration_id VARCHAR,
    token_id VARCHAR,

    PRIMARY KEY(service, registration_id)
);

INSERT INTO push_notification_registrations SELECT 'gcm', gcm_id, token_id from gcm_registrations;
INSERT INTO push_notification_registrations SELECT 'apn', apn_id, token_id from apn_registrations;

DROP TABLE gcm_registrations;
DROP TABLE apn_registrations;
