package com.finpay.api.service;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;

@Component
@ConditionalOnProperty(
        name = {
                "finpay.kafka.enabled",
                "finpay.kafka.publisher-enabled",
                "finpay.kafka.scheduler-enabled"
        },
        havingValue = "true")
public class KafkaPublisherScheduler {

    private static final Logger LOGGER = LoggerFactory.getLogger(KafkaPublisherScheduler.class);

    private final KafkaOutboxPublisher publisher;

    public KafkaPublisherScheduler(KafkaOutboxPublisher publisher) {
        this.publisher = publisher;
    }

    @Scheduled(
            initialDelayString = "${finpay.kafka.scheduler-initial-delay-ms:5000}",
            fixedDelayString = "${finpay.kafka.scheduler-delay-ms:3000}")
    public void publish() {
        try {
            publisher.publishPendingEvents();
        } catch (RuntimeException exception) {
            LOGGER.error("Kafka publication cycle failed", exception);
        }
    }
}
