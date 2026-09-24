package com.finpay.listener.model;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(
        name = "consumed_payment_events",
        uniqueConstraints = @UniqueConstraint(
                name = "UQ_consumed_payment_events_position",
                columnNames = {"topic_name", "partition_id", "record_offset"}))
public class ConsumedPaymentEvent {

    @Id
    @Column(name = "event_id", nullable = false)
    private UUID eventId;

    @Column(name = "merchant_id", nullable = false)
    private Long merchantId;

    @Column(name = "payment_id", nullable = false)
    private Long paymentId;

    @Column(name = "event_type", nullable = false, length = 40)
    private String eventType;

    @Column(nullable = false, precision = 19, scale = 2)
    private BigDecimal amount;

    @Column(nullable = false, length = 3)
    private String currency;

    @Column(name = "payment_status", nullable = false, length = 20)
    private String paymentStatus;

    @Column(nullable = false, length = 4000)
    private String payload;

    @Column(name = "topic_name", nullable = false, length = 255)
    private String topic;

    @Column(name = "partition_id", nullable = false)
    private int partition;

    @Column(name = "record_offset", nullable = false)
    private long offset;

    @Column(name = "occurred_at", nullable = false)
    private Instant occurredAt;

    @Column(name = "consumed_at", nullable = false)
    private Instant consumedAt;

    protected ConsumedPaymentEvent() {
        // Required by JPA.
    }

    public ConsumedPaymentEvent(
            UUID eventId,
            Long merchantId,
            Long paymentId,
            String eventType,
            BigDecimal amount,
            String currency,
            String paymentStatus,
            String payload,
            String topic,
            int partition,
            long offset,
            Instant occurredAt,
            Instant consumedAt) {
        this.eventId = eventId;
        this.merchantId = merchantId;
        this.paymentId = paymentId;
        this.eventType = eventType;
        this.amount = amount;
        this.currency = currency;
        this.paymentStatus = paymentStatus;
        this.payload = payload;
        this.topic = topic;
        this.partition = partition;
        this.offset = offset;
        this.occurredAt = occurredAt;
        this.consumedAt = consumedAt;
    }

    public UUID getEventId() {
        return eventId;
    }

    public Long getMerchantId() {
        return merchantId;
    }

    public Long getPaymentId() {
        return paymentId;
    }

    public String getEventType() {
        return eventType;
    }

    public BigDecimal getAmount() {
        return amount;
    }

    public String getCurrency() {
        return currency;
    }

    public String getPaymentStatus() {
        return paymentStatus;
    }

    public String getPayload() {
        return payload;
    }

    public String getTopic() {
        return topic;
    }

    public int getPartition() {
        return partition;
    }

    public long getOffset() {
        return offset;
    }

    public Instant getOccurredAt() {
        return occurredAt;
    }

    public Instant getConsumedAt() {
        return consumedAt;
    }
}
