CREATE TABLE dbo.consumed_payment_events (
    event_id UNIQUEIDENTIFIER NOT NULL,
    merchant_id BIGINT NOT NULL,
    payment_id BIGINT NOT NULL,
    event_type VARCHAR(40) NOT NULL,
    amount DECIMAL(19, 2) NOT NULL,
    currency VARCHAR(3) NOT NULL,
    payment_status VARCHAR(20) NOT NULL,
    payload VARCHAR(4000) NOT NULL,
    topic_name VARCHAR(255) NOT NULL,
    partition_id INT NOT NULL,
    record_offset BIGINT NOT NULL,
    occurred_at DATETIMEOFFSET(7) NOT NULL,
    consumed_at DATETIMEOFFSET(7) NOT NULL,
    CONSTRAINT PK_consumed_payment_events PRIMARY KEY (event_id),
    CONSTRAINT UQ_consumed_payment_events_position
        UNIQUE (topic_name, partition_id, record_offset),
    CONSTRAINT CK_consumed_payment_events_partition CHECK (partition_id >= 0),
    CONSTRAINT CK_consumed_payment_events_offset CHECK (record_offset >= 0)
);
GO

CREATE INDEX IX_consumed_payment_events_merchant
ON dbo.consumed_payment_events (merchant_id, consumed_at DESC);
GO

CREATE INDEX IX_consumed_payment_events_payment
ON dbo.consumed_payment_events (payment_id, occurred_at);
