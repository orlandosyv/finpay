package com.finpay.api.model;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.EnumType;
import jakarta.persistence.Enumerated;
import jakarta.persistence.FetchType;
import jakarta.persistence.Id;
import jakarta.persistence.JoinColumn;
import jakarta.persistence.ManyToOne;
import jakarta.persistence.MapsId;
import jakarta.persistence.OneToOne;
import jakarta.persistence.Table;

@Entity
@Table(name = "kafka_publications")
public class KafkaPublication {

    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @OneToOne(fetch = FetchType.LAZY, optional = false)
    @MapsId
    @JoinColumn(name = "event_id")
    private OutboxEvent event;

    @ManyToOne(fetch = FetchType.LAZY, optional = false)
    @JoinColumn(name = "merchant_id", nullable = false)
    private Merchant merchant;

    @Enumerated(EnumType.STRING)
    @Column(nullable = false, length = 20)
    private KafkaPublicationStatus status;

    @Column(nullable = false)
    private int attempts;

    @Column(name = "available_at", nullable = false)
    private Instant availableAt;

    @Column(name = "published_at")
    private Instant publishedAt;

    @Column(name = "partition_id")
    private Integer partition;

    @Column(name = "record_offset")
    private Long offset;

    @Column(name = "last_error", length = 1000)
    private String lastError;

    @Column(name = "created_at", nullable = false, updatable = false)
    private Instant createdAt;

    protected KafkaPublication() {
        // No-argument constructor required by JPA.
    }

    public KafkaPublication(OutboxEvent event, Merchant merchant, Instant availableAt) {
        this.event = event;
        this.merchant = merchant;
        this.status = KafkaPublicationStatus.PENDING;
        this.attempts = 0;
        this.availableAt = availableAt;
        this.createdAt = availableAt;
    }

    public UUID getEventId() {
        return eventId;
    }

    public OutboxEvent getEvent() {
        return event;
    }

    public Long getMerchantId() {
        return merchant.getId();
    }

    public KafkaPublicationStatus getStatus() {
        return status;
    }

    public int getAttempts() {
        return attempts;
    }

    public Instant getAvailableAt() {
        return availableAt;
    }

    public Instant getPublishedAt() {
        return publishedAt;
    }

    public Integer getPartition() {
        return partition;
    }

    public Long getOffset() {
        return offset;
    }

    public String getLastError() {
        return lastError;
    }

    public Instant getCreatedAt() {
        return createdAt;
    }

    public void markPublished(Instant publishedAt, int partition, long offset) {
        status = KafkaPublicationStatus.PUBLISHED;
        attempts++;
        this.publishedAt = publishedAt;
        this.partition = partition;
        this.offset = offset;
        lastError = null;
    }

    public void scheduleRetry(Instant nextAttemptAt, String error) {
        status = KafkaPublicationStatus.PENDING;
        attempts++;
        availableAt = nextAttemptAt;
        lastError = error;
    }

    public void markFailed(String error) {
        status = KafkaPublicationStatus.FAILED;
        attempts++;
        lastError = error;
    }
}
