package com.finpay.listener.model;

import java.time.Instant;
import java.util.UUID;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.GeneratedValue;
import jakarta.persistence.GenerationType;
import jakarta.persistence.Id;
import jakarta.persistence.Table;
import jakarta.persistence.UniqueConstraint;

@Entity
@Table(
        name = "dead_letter_payment_events",
        uniqueConstraints = @UniqueConstraint(
                name = "UQ_dead_letter_payment_events_position",
                columnNames = {"dlt_topic", "dlt_partition", "dlt_offset"}))
public class DeadLetterPaymentEvent {

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    private Long id;

    @Column(name = "event_id")
    private UUID eventId;

    @Column(name = "merchant_id")
    private Long merchantId;

    @Column(name = "original_topic", nullable = false, length = 255)
    private String originalTopic;

    @Column(name = "original_partition", nullable = false)
    private int originalPartition;

    @Column(name = "original_offset", nullable = false)
    private long originalOffset;

    @Column(name = "dlt_topic", nullable = false, length = 255)
    private String dltTopic;

    @Column(name = "dlt_partition", nullable = false)
    private int dltPartition;

    @Column(name = "dlt_offset", nullable = false)
    private long dltOffset;

    @Column(nullable = false, columnDefinition = "nvarchar(max)")
    private String payload;

    @Column(name = "exception_class", length = 500)
    private String exceptionClass;

    @Column(name = "exception_message", length = 2000)
    private String exceptionMessage;

    @Column(name = "failed_at", nullable = false)
    private Instant failedAt;

    protected DeadLetterPaymentEvent() {
        // Required by JPA.
    }

    public DeadLetterPaymentEvent(
            UUID eventId,
            Long merchantId,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String dltTopic,
            int dltPartition,
            long dltOffset,
            String payload,
            String exceptionClass,
            String exceptionMessage,
            Instant failedAt) {
        this.eventId = eventId;
        this.merchantId = merchantId;
        this.originalTopic = originalTopic;
        this.originalPartition = originalPartition;
        this.originalOffset = originalOffset;
        this.dltTopic = dltTopic;
        this.dltPartition = dltPartition;
        this.dltOffset = dltOffset;
        this.payload = payload;
        this.exceptionClass = exceptionClass;
        this.exceptionMessage = exceptionMessage;
        this.failedAt = failedAt;
    }

    public Long getId() { return id; }
    public UUID getEventId() { return eventId; }
    public Long getMerchantId() { return merchantId; }
    public String getOriginalTopic() { return originalTopic; }
    public int getOriginalPartition() { return originalPartition; }
    public long getOriginalOffset() { return originalOffset; }
    public String getDltTopic() { return dltTopic; }
    public int getDltPartition() { return dltPartition; }
    public long getDltOffset() { return dltOffset; }
    public String getPayload() { return payload; }
    public String getExceptionClass() { return exceptionClass; }
    public String getExceptionMessage() { return exceptionMessage; }
    public Instant getFailedAt() { return failedAt; }
}
