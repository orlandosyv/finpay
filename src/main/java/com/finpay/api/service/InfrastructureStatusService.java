package com.finpay.api.service;

import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.security.access.prepost.PreAuthorize;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.finpay.api.context.CurrentMerchantProvider;
import com.finpay.api.dto.InfrastructureStatusResponse;
import com.finpay.api.dto.InfrastructureStatusResponse.KafkaEventStatus;
import com.finpay.api.dto.InfrastructureStatusResponse.KafkaStatus;
import com.finpay.api.dto.InfrastructureStatusResponse.PipelineStage;
import com.finpay.api.dto.InfrastructureStatusResponse.WebhookStatus;
import com.finpay.api.model.KafkaPublication;
import com.finpay.api.model.KafkaPublicationStatus;
import com.finpay.api.repository.KafkaPublicationRepository;
import com.finpay.api.repository.WebhookEndpointRepository;
import com.finpay.api.service.KafkaClusterProbe.KafkaProbeResult;

@Service
public class InfrastructureStatusService {

    private final CurrentMerchantProvider currentMerchantProvider;
    private final WebhookEndpointRepository webhookEndpointRepository;
    private final KafkaPublicationRepository kafkaPublicationRepository;
    private final KafkaClusterProbe kafkaClusterProbe;
    private final String topicName;
    private final boolean publishingEnabled;

    public InfrastructureStatusService(
            CurrentMerchantProvider currentMerchantProvider,
            WebhookEndpointRepository webhookEndpointRepository,
            KafkaPublicationRepository kafkaPublicationRepository,
            KafkaClusterProbe kafkaClusterProbe,
            @Value("${finpay.kafka.payment-events-topic}") String topicName,
            @Value("${finpay.kafka.publisher-enabled}") boolean publishingEnabled) {
        this.currentMerchantProvider = currentMerchantProvider;
        this.webhookEndpointRepository = webhookEndpointRepository;
        this.kafkaPublicationRepository = kafkaPublicationRepository;
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
        long totalEvents = kafkaPublicationRepository.countByMerchant_Id(merchantId);
        long pendingEvents = kafkaPublicationRepository.countByMerchant_IdAndStatus(
                merchantId, KafkaPublicationStatus.PENDING);
        long publishedEvents = kafkaPublicationRepository.countByMerchant_IdAndStatus(
                merchantId, KafkaPublicationStatus.PUBLISHED);
        long failedEvents = kafkaPublicationRepository.countByMerchant_IdAndStatus(
                merchantId, KafkaPublicationStatus.FAILED);
        List<KafkaEventStatus> recentEvents = kafkaPublicationRepository
                .findTop10ByMerchant_IdOrderByCreatedAtDesc(merchantId)
                .stream()
                .map(this::toKafkaEventStatus)
                .toList();

        KafkaStatus kafkaStatus = new KafkaStatus(
                kafka.status(), topicName, kafka.topicAvailable(), kafka.partitions(),
                publishingEnabled, kafka.clusterId(), totalEvents, pendingEvents,
                publishedEvents, failedEvents, recentEvents, kafkaHint(kafka));
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
                                publishingEnabled
                                        ? "The outbox publisher sends events for independent internal consumers."
                                        : "The broker and topic are prepared, but publishing is disabled.")));
    }

    private KafkaEventStatus toKafkaEventStatus(KafkaPublication publication) {
        return new KafkaEventStatus(
                publication.getEventId().toString(),
                publication.getEvent().getAggregateId(),
                publication.getEvent().getEventType().getEventName(),
                publication.getStatus().name(),
                publication.getAttempts(),
                publication.getPartition(),
                publication.getOffset(),
                publication.getCreatedAt(),
                publication.getPublishedAt(),
                publication.getStatus() == KafkaPublicationStatus.PENDING
                        ? publication.getAvailableAt()
                        : null,
                publication.getLastError());
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
                    ? "The publisher is active. Each payment event is sent from the durable outbox."
                    : "The broker and topic are ready. No payment events are published until step 2.";
            case "DISABLED" -> "Kafka is disabled. Start FinPay with Docker Compose to enable the lab.";
            default -> "Kafka is enabled, but the API cannot currently reach the broker.";
        };
    }
}
