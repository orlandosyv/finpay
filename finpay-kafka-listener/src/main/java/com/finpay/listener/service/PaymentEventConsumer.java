package com.finpay.listener.service;

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

import com.finpay.listener.dto.PaymentEventMessage;
import com.finpay.listener.dto.PaymentEventMessage.PaymentData;
import com.finpay.listener.model.ConsumedPaymentEvent;
import com.finpay.listener.repository.ConsumedPaymentEventRepository;

import tools.jackson.core.JacksonException;
import tools.jackson.databind.ObjectMapper;

@Service
public class PaymentEventConsumer {

    private static final Logger LOGGER = LoggerFactory.getLogger(PaymentEventConsumer.class);

    private final ConsumedPaymentEventRepository repository;
    private final ObjectMapper objectMapper;

    public PaymentEventConsumer(
            ConsumedPaymentEventRepository repository,
            ObjectMapper objectMapper) {
        this.repository = repository;
        this.objectMapper = objectMapper;
    }

    @KafkaListener(
            id = "finpay-payment-audit-listener",
            topics = "${finpay.kafka.topic}",
            groupId = "${finpay.kafka.consumer-group}",
            concurrency = "${finpay.kafka.concurrency:3}")
    @Transactional
    public void consume(ConsumerRecord<String, String> record) {
        PaymentEventMessage message = deserialize(record.value());
        UUID eventId = UUID.fromString(requiredHeader(record, "finpay-event-id"));
        Long merchantId = Long.valueOf(requiredHeader(record, "finpay-merchant-id"));

        if (!eventId.equals(message.id())) {
            throw new IllegalArgumentException("Kafka header and payload event IDs do not match");
        }
        if (repository.existsById(eventId)) {
            LOGGER.info("Ignoring duplicate payment event {}", eventId);
            return;
        }

        PaymentData payment = requirePayment(message);
        repository.save(new ConsumedPaymentEvent(
                eventId,
                merchantId,
                payment.id(),
                message.type(),
                payment.amount(),
                payment.currency(),
                payment.status(),
                record.value(),
                record.topic(),
                record.partition(),
                record.offset(),
                message.createdAt(),
                Instant.now()));

        LOGGER.info(
                "Consumed event {} for payment {} from partition {} offset {}",
                eventId,
                payment.id(),
                record.partition(),
                record.offset());
    }

    private PaymentEventMessage deserialize(String payload) {
        try {
            return objectMapper.readValue(payload, PaymentEventMessage.class);
        } catch (JacksonException exception) {
            throw new IllegalArgumentException("Payment event payload is invalid", exception);
        }
    }

    private PaymentData requirePayment(PaymentEventMessage message) {
        if (message.id() == null
                || message.type() == null
                || message.createdAt() == null
                || message.data() == null
                || message.data().payment() == null) {
            throw new IllegalArgumentException("Payment event is missing required fields");
        }
        return message.data().payment();
    }

    private String requiredHeader(ConsumerRecord<String, String> record, String name) {
        Header header = record.headers().lastHeader(name);
        if (header == null) {
            throw new IllegalArgumentException("Missing Kafka header: " + name);
        }
        return new String(header.value(), StandardCharsets.UTF_8);
    }
}
