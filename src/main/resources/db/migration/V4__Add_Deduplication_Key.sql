ALTER TABLE alerts ADD COLUMN deduplication_key VARCHAR(255);
UPDATE alerts SET deduplication_key = alert_id::varchar;
ALTER TABLE alerts ALTER COLUMN deduplication_key SET NOT NULL;
ALTER TABLE alerts ADD CONSTRAINT uk_alerts_dedup_key UNIQUE (deduplication_key);
