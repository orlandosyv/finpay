package com.finpay.listener.service;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.common.header.Header;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.finpay.listener.model.DeadLetterPaymentEvent;
import com.finpay.listener.repository.DeadLetterPaymentEventRepository;

@Service
public class DeadLetterEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(DeadLetterEventConsumer.class);
    private static final String ORIGINAL_TOPIC = "kafka_dlt-original-topic";
    private static final String ORIGINAL_PARTITION = "kafka_dlt-original-partition";
    private static final String ORIGINAL_OFFSET = "kafka_dlt-original-offset";
    private static final String EXCEPTION_CLASS = "kafka_dlt-exception-fqcn";
    private static final String EXCEPTION_MESSAGE = "kafka_dlt-exception-message";

    private final DeadLetterPaymentEventRepository repository;

    public DeadLetterEventConsumer(DeadLetterPaymentEventRepository repository) {
        this.repository = repository;
    }

    @KafkaListener(
            id = "finpay-payment-audit-dlt-listener",
            topics = "${finpay.kafka.dlt-topic}",
            groupId = "${finpay.kafka.dlt-consumer-group}",
            concurrency = "${finpay.kafka.dlt-concurrency:1}",
            containerFactory = "dltKafkaListenerContainerFactory")
    @Transactional
    public void consume(ConsumerRecord<String, String> record) {
        if (repository.existsByDltTopicAndDltPartitionAndDltOffset(
                record.topic(), record.partition(), record.offset())) {
            return;
        }

        DeadLetterPaymentEvent event = new DeadLetterPaymentEvent(
                uuidHeader(record, "finpay-event-id"),
                longTextHeader(record, "finpay-merchant-id"),
                textHeader(record, ORIGINAL_TOPIC, record.topic()),
                intHeader(record, ORIGINAL_PARTITION, record.partition()),
                longHeader(record, ORIGINAL_OFFSET, -1L),
                record.topic(),
                record.partition(),
                record.offset(),
                record.value(),
                textHeader(record, EXCEPTION_CLASS, null),
                textHeader(record, EXCEPTION_MESSAGE, "Unknown listener failure"),
                Instant.now());
        repository.save(event);

        LOGGER.warn(
                "Stored dead-letter event {} from {}-{} offset {}",
                event.getEventId(),
                event.getOriginalTopic(),
                event.getOriginalPartition(),
                event.getOriginalOffset());
    }

    private UUID uuidHeader(ConsumerRecord<String, String> record, String name) {
        String value = textHeader(record, name, null);
        if (value == null) {
            return null;
        }
        try {
            return UUID.fromString(value);
        } catch (IllegalArgumentException exception) {
            return null;
        }
    }

    private Long longTextHeader(ConsumerRecord<String, String> record, String name) {
        String value = textHeader(record, name, null);
        if (value == null) {
            return null;
        }
        try {
            return Long.valueOf(value);
        } catch (NumberFormatException exception) {
            return null;
        }
    }

    private String textHeader(
            ConsumerRecord<String, String> record,
            String name,
            String fallback) {
        Header header = record.headers().lastHeader(name);
        return header == null
                ? fallback
                : new String(header.value(), StandardCharsets.UTF_8);
    }

    private int intHeader(ConsumerRecord<String, String> record, String name, int fallback) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return fallback;
        }
        if (header.value().length == Integer.BYTES) {
            return ByteBuffer.wrap(header.value()).getInt();
        }
        try {
            return Integer.parseInt(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }

    private long longHeader(ConsumerRecord<String, String> record, String name, long fallback) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            return fallback;
        }
        if (header.value().length == Long.BYTES) {
            return ByteBuffer.wrap(header.value()).getLong();
        }
        try {
            return Long.parseLong(new String(header.value(), StandardCharsets.UTF_8));
        } catch (NumberFormatException exception) {
            return fallback;
        }
    }
}
