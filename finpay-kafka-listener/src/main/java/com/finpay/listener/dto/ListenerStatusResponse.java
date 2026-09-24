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
}
