package com.finpay.api.dto;

import java.time.Instant;
import java.util.List;

public record InfrastructureStatusResponse(
        Instant checkedAt,
        KafkaStatus kafka,
        WebhookStatus webhooks,
        List<PipelineStage> pipeline) {

    public record KafkaStatus(
            String status,
            String topic,
            boolean topicAvailable,
            int partitions,
            boolean publishingEnabled,
            String clusterId,
            long totalEvents,
            long pendingEvents,
            long publishedEvents,
            long failedEvents,
            List<KafkaEventStatus> recentEvents,
            KafkaConsumerStatus consumer,
            String hint) {
    }

    public record KafkaConsumerStatus(
            Instant checkedAt,
            String status,
            String application,
            String topic,
            String consumerGroup,
            int concurrency,
            long consumedEvents,
            long consumerGroupLag,
            Instant lastConsumedAt,
            List<KafkaConsumedEvent> recentEvents,
            KafkaDeadLetterStatus deadLetter,
            String hint) {
    }

    public record KafkaDeadLetterStatus(
            String topic,
            int maxRetries,
            long retryIntervalMs,
            long events,
            List<KafkaDeadLetterEvent> recentEvents) {
    }

    public record KafkaDeadLetterEvent(
            String eventId,
            String originalTopic,
            int originalPartition,
            long originalOffset,
            String exceptionClass,
            String exceptionMessage,
            Instant failedAt) {
    }

    public record KafkaConsumedEvent(
            String eventId,
            Long paymentId,
            String eventType,
            String paymentStatus,
            int partition,
            long offset,
            Instant occurredAt,
            Instant consumedAt) {
    }

    public record KafkaEventStatus(
            String eventId,
            Long paymentId,
            String eventType,
            String status,
            int attempts,
            Integer partition,
            Long offset,
            Instant createdAt,
            Instant publishedAt,
            Instant nextAttemptAt,
            String lastError) {
    }

    public record WebhookStatus(
            String status,
            long activeEndpoints,
            String deliveryMode,
            String hint) {
    }

    public record PipelineStage(
            int order,
            String name,
            String technology,
            String state,
            String hint) {
    }
}
