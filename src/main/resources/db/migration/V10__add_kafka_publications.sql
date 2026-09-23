CREATE TABLE dbo.kafka_publications (
    event_id UNIQUEIDENTIFIER NOT NULL,
    merchant_id BIGINT NOT NULL,
    status VARCHAR(20) NOT NULL,
    attempts INT NOT NULL,
    available_at DATETIMEOFFSET(7) NOT NULL,
    published_at DATETIMEOFFSET(7) NULL,
    partition_id INT NULL,
    record_offset BIGINT NULL,
    last_error VARCHAR(1000) NULL,
    created_at DATETIMEOFFSET(7) NOT NULL,
    CONSTRAINT PK_kafka_publications PRIMARY KEY (event_id),
    CONSTRAINT CK_kafka_publications_status
        CHECK (status IN ('PENDING', 'PUBLISHED', 'FAILED')),
    CONSTRAINT CK_kafka_publications_attempts CHECK (attempts >= 0),
    CONSTRAINT CK_kafka_publications_partition CHECK (partition_id IS NULL OR partition_id >= 0),
    CONSTRAINT CK_kafka_publications_offset CHECK (record_offset IS NULL OR record_offset >= 0),
    CONSTRAINT FK_kafka_publications_events
        FOREIGN KEY (event_id) REFERENCES dbo.outbox_events(id) ON DELETE CASCADE,
    CONSTRAINT FK_kafka_publications_merchants
        FOREIGN KEY (merchant_id) REFERENCES dbo.merchants(id)
);
GO

INSERT INTO dbo.kafka_publications (
    event_id,
    merchant_id,
    status,
    attempts,
    available_at,
    published_at,
    partition_id,
    record_offset,
    last_error,
    created_at
)
SELECT
    id,
    merchant_id,
    'PENDING',
    0,
    created_at,
    NULL,
    NULL,
    NULL,
    NULL,
    created_at
FROM dbo.outbox_events;
GO

CREATE INDEX IX_kafka_publications_pending
ON dbo.kafka_publications (status, available_at, created_at);
GO

CREATE INDEX IX_kafka_publications_merchant
ON dbo.kafka_publications (merchant_id, created_at DESC);
