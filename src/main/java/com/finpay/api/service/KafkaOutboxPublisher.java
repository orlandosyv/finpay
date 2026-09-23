package com.finpay.api.service;

import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;
import org.springframework.stereotype.Service;

import com.finpay.api.model.KafkaPublication;
import com.finpay.api.model.KafkaPublicationStatus;
import com.finpay.api.model.OutboxEvent;
import com.finpay.api.repository.KafkaPublicationRepository;

@Service
public class KafkaOutboxPublisher {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaOutboxPublisher.class);

    private final KafkaPublicationRepository repository;
    private final KafkaTemplate<String, String> kafkaTemplate;
    private final KafkaPublicationStateService stateService;
    private final String topic;
    private final long sendTimeoutSeconds;

    public KafkaOutboxPublisher(
            KafkaPublicationRepository repository,
            KafkaTemplate<String, String> kafkaTemplate,
            KafkaPublicationStateService stateService,
            @Value("${finpay.kafka.payment-events-topic}") String topic,
            @Value("${finpay.kafka.send-timeout-seconds:10}") long sendTimeoutSeconds) {
        this.repository = repository;
        this.kafkaTemplate = kafkaTemplate;
        this.stateService = stateService;
        this.topic = topic;
        this.sendTimeoutSeconds = sendTimeoutSeconds;
    }

    public int publishPendingEvents() {
        List<KafkaPublication> publications = repository
                .findTop20ByStatusAndAvailableAtLessThanEqualOrderByCreatedAtAsc(
                        KafkaPublicationStatus.PENDING,
                        Instant.now());
        publications.forEach(this::publish);
        return publications.size();
    }

    private void publish(KafkaPublication publication) {
        OutboxEvent event = publication.getEvent();
        ProducerRecord<String, String> record = new ProducerRecord<>(
                topic,
                null,
                event.getCreatedAt().toEpochMilli(),
                event.getAggregateId().toString(),
                event.getPayload());
        addHeader(record, "finpay-event-id", event.getId().toString());
        addHeader(record, "finpay-event-type", event.getEventType().getEventName());
        addHeader(record, "finpay-merchant-id", event.getMerchantId().toString());

        try {
            SendResult<String, String> result = kafkaTemplate.send(record)
                    .get(sendTimeoutSeconds, TimeUnit.SECONDS);
            stateService.markPublished(
                    event.getId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
            LOGGER.info(
                    "Published event {} to Kafka partition {} at offset {}",
                    event.getId(),
                    result.getRecordMetadata().partition(),
                    result.getRecordMetadata().offset());
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            stateService.retryOrFail(event.getId(), "Kafka publication was interrupted");
        } catch (Exception exception) {
            stateService.retryOrFail(event.getId(), rootMessage(exception));
            LOGGER.warn("Could not publish event {} to Kafka", event.getId(), exception);
        }
    }

    private void addHeader(ProducerRecord<String, String> record, String name, String value) {
        record.headers().add(name, value.getBytes(StandardCharsets.UTF_8));
    }

    private String rootMessage(Exception exception) {
        Throwable cause = exception;
        while (cause.getCause() != null) {
            cause = cause.getCause();
        }
        return cause.getMessage() == null ? cause.getClass().getSimpleName() : cause.getMessage();
    }
}
