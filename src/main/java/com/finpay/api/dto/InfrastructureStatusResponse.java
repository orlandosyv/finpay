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
            String hint) {
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
