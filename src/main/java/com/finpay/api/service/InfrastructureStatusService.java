package com.finpay.api.service;

import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.finpay.api.context.CurrentMerchantProvider;
import com.finpay.api.dto.InfrastructureStatusResponse;
import com.finpay.api.dto.InfrastructureStatusResponse.KafkaStatus;
import com.finpay.api.dto.InfrastructureStatusResponse.PipelineStage;
import com.finpay.api.dto.InfrastructureStatusResponse.WebhookStatus;
import com.finpay.api.repository.WebhookEndpointRepository;
import com.finpay.api.service.KafkaClusterProbe.KafkaProbeResult;

@Service
public class InfrastructureStatusService {

    private final CurrentMerchantProvider currentMerchantProvider;
    private final WebhookEndpointRepository webhookEndpointRepository;
    private final KafkaClusterProbe kafkaClusterProbe;
    private final String topicName;
    private final boolean publishingEnabled;

    public InfrastructureStatusService(
            CurrentMerchantProvider currentMerchantProvider,
            WebhookEndpointRepository webhookEndpointRepository,
            KafkaClusterProbe kafkaClusterProbe,
            @Value("${finpay.kafka.payment-events-topic}") String topicName,
            @Value("${finpay.kafka.publisher-enabled}") boolean publishingEnabled) {
        this.currentMerchantProvider = currentMerchantProvider;
        this.webhookEndpointRepository = webhookEndpointRepository;
        this.kafkaClusterProbe = kafkaClusterProbe;
        this.topicName = topicName;
        this.publishingEnabled = publishingEnabled;
    }

    @PreAuthorize("hasRole('MERCHANT_ADMIN')")
    @Transactional(readOnly = true)
    public InfrastructureStatusResponse getStatus() {
        Long merchantId = currentMerchantProvider.getCurrentMerchantId();
        long activeEndpoints = webhookEndpointRepository.countByMerchantIdAndActiveTrue(merchantId);
        KafkaProbeResult kafka = kafkaClusterProbe.inspect(topicName);

        KafkaStatus kafkaStatus = new KafkaStatus(
                kafka.status(), topicName, kafka.topicAvailable(), kafka.partitions(),
                publishingEnabled, kafka.clusterId(), kafkaHint(kafka));
        WebhookStatus webhookStatus = new WebhookStatus(
                activeEndpoints > 0 ? "ACTIVE" : "NOT_CONFIGURED",
                activeEndpoints,
                "SIGNED_HTTP_WITH_RETRIES",
                activeEndpoints > 0
                        ? "Payment events can be delivered to the merchant's HTTP endpoint."
                        : "Add a webhook endpoint to receive external payment notifications.");

        return new InfrastructureStatusResponse(
                Instant.now(),
                kafkaStatus,
                webhookStatus,
                List.of(
                        new PipelineStage(1, "Payment transaction", "SQL Server", "ACTIVE",
                                "The database remains the source of truth."),
                        new PipelineStage(2, "Durable event record", "Transactional outbox", "ACTIVE",
                                "The payment and its event are committed together."),
                        new PipelineStage(3, "External merchant notification", "Webhooks",
                                activeEndpoints > 0 ? "ACTIVE" : "WAITING_FOR_ENDPOINT",
                                "Signed HTTP deliveries retry when the merchant is unavailable."),
                        new PipelineStage(4, "Internal event stream", "Apache Kafka", kafkaStage(kafka),
                                "The broker and topic are prepared; payment publishing starts in step 2.")));
    }

    private String kafkaStage(KafkaProbeResult kafka) {
        if (!kafka.topicAvailable()) {
            return kafka.status();
        }
        return publishingEnabled ? "ACTIVE" : "READY_NOT_PUBLISHING";
    }

    private String kafkaHint(KafkaProbeResult kafka) {
        return switch (kafka.status()) {
            case "CONNECTED" -> publishingEnabled
                    ? "FinPay is connected and payment events are being published."
                    : "The broker and topic are ready. No payment events are published until step 2.";
            case "DISABLED" -> "Kafka is disabled. Start FinPay with Docker Compose to enable the lab.";
            default -> "Kafka is enabled, but the API cannot currently reach the broker.";
        };
    }
}
