CREATE TABLE transactions (
    transaction_id UUID PRIMARY KEY,
    event_id UUID UNIQUE NOT NULL,
    source_account_id UUID NOT NULL,
    destination_account_id UUID NOT NULL,
    amount_minor BIGINT NOT NULL,
    currency VARCHAR(3) NOT NULL,
    device_id VARCHAR(255) NOT NULL,
    ip_hash VARCHAR(255) NOT NULL,
    occurred_at TIMESTAMP WITH TIME ZONE NOT NULL,
    ingested_at TIMESTAMP WITH TIME ZONE NOT NULL,
    processed_at TIMESTAMP WITH TIME ZONE NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

CREATE INDEX idx_txn_source_time ON transactions(source_account_id, occurred_at);
CREATE INDEX idx_txn_dest_time ON transactions(destination_account_id, occurred_at);
CREATE INDEX idx_txn_device_time ON transactions(device_id, occurred_at);
CREATE INDEX idx_txn_ip_time ON transactions(ip_hash, occurred_at);
