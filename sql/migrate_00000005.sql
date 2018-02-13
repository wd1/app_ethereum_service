ALTER TABLE transactions RENAME COLUMN sender_token_id TO sender_toshi_id;
ALTER TABLE notification_registrations RENAME COLUMN token_id TO toshi_id;
