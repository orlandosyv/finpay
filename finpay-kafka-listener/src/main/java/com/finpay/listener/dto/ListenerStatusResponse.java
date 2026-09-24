package com.finpay.listener.dto;

import java.time.Instant;
import java.util.List;

public record ListenerStatusResponse(
        Instant checkedAt,
        String status,
        String application,
        String topic,
        String consumerGroup,
        int concurrency,
        long consumedEvents,
        long consumerGroupLag,
        Instant lastConsumedAt,
        List<ConsumedEventResponse> recentEvents,
        DeadLetterStatus deadLetter,
        String hint) {

    public record ConsumedEventResponse(
            String eventId,
            Long paymentId,
            String eventType,
            String paymentStatus,
            int partition,
            long offset,
            Instant occurredAt,
            Instant consumedAt) {
    }

    public record DeadLetterStatus(
            String topic,
            int maxRetries,
            long retryIntervalMs,
            long events,
            List<DeadLetterEventResponse> recentEvents) {
    }

    public record DeadLetterEventResponse(
            String eventId,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String exceptionClass,
            String exceptionMessage,
            Instant failedAt) {
    }
}
