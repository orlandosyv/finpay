CREATE TABLE dbo.dead_letter_payment_events (
    id BIGINT IDENTITY(1,1) NOT NULL,
    event_id UNIQUEIDENTIFIER NULL,
    merchant_id BIGINT NULL,
    original_topic VARCHAR(255) NOT NULL,
    original_partition INT NOT NULL,
    original_offset BIGINT NOT NULL,
    dlt_topic VARCHAR(255) NOT NULL,
    dlt_partition INT NOT NULL,
    dlt_offset BIGINT NOT NULL,
    payload NVARCHAR(MAX) NOT NULL,
    exception_class VARCHAR(500) NULL,
    exception_message NVARCHAR(2000) NULL,
    failed_at DATETIMEOFFSET(7) NOT NULL,
    CONSTRAINT PK_dead_letter_payment_events PRIMARY KEY (id),
    CONSTRAINT UQ_dead_letter_payment_events_position
        UNIQUE (dlt_topic, dlt_partition, dlt_offset)
);

CREATE INDEX IX_dead_letter_payment_events_merchant_failed
    ON dbo.dead_letter_payment_events (merchant_id, failed_at DESC);
