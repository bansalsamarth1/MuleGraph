CREATE TABLE alerts (
    alert_id UUID PRIMARY KEY,
    rule_type VARCHAR(50) NOT NULL,
    primary_account_id UUID NOT NULL,
    status VARCHAR(20) NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE NOT NULL DEFAULT NOW()
);

CREATE TABLE alert_accounts (
    alert_id UUID NOT NULL,
    account_id UUID NOT NULL,
    PRIMARY KEY (alert_id, account_id),
    CONSTRAINT fk_alert_accounts_alert_id FOREIGN KEY (alert_id) REFERENCES alerts(alert_id) ON DELETE CASCADE
);

CREATE TABLE alert_transactions (
    alert_id UUID NOT NULL,
    transaction_id UUID NOT NULL,
    PRIMARY KEY (alert_id, transaction_id),
    CONSTRAINT fk_alert_transactions_alert_id FOREIGN KEY (alert_id) REFERENCES alerts(alert_id) ON DELETE CASCADE
);
