package com.finpay.listener.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Instant;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.finpay.listener.dto.PaymentEventMessage;
import com.finpay.listener.dto.PaymentEventMessage.EventData;
import com.finpay.listener.dto.PaymentEventMessage.PaymentData;
import com.finpay.listener.model.ConsumedPaymentEvent;
import com.finpay.listener.repository.ConsumedPaymentEventRepository;

import tools.jackson.databind.ObjectMapper;

@ExtendWith(MockitoExtension.class)
class PaymentEventConsumerTest {

    @Mock
    private ConsumedPaymentEventRepository repository;

    @Mock
    private ObjectMapper objectMapper;

    private PaymentEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new PaymentEventConsumer(repository, objectMapper);
    }

    @Test
    void storesAnAuditProjectionWithKafkaPosition() throws Exception {
        UUID eventId = UUID.randomUUID();
        ConsumerRecord<String, String> record = record(eventId);
        when(objectMapper.readValue("payload", PaymentEventMessage.class))
                .thenReturn(message(eventId));

        consumer.consume(record);

        ArgumentCaptor<ConsumedPaymentEvent> captor = ArgumentCaptor.forClass(
                ConsumedPaymentEvent.class);
        verify(repository).save(captor.capture());
        ConsumedPaymentEvent saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(eventId);
        assertThat(saved.getMerchantId()).isEqualTo(7L);
        assertThat(saved.getPaymentId()).isEqualTo(91L);
        assertThat(saved.getEventType()).isEqualTo("payment.approved");
        assertThat(saved.getPartition()).isEqualTo(2);
        assertThat(saved.getOffset()).isEqualTo(14L);
    }

    @Test
    void ignoresAnAlreadyConsumedEventId() throws Exception {
        UUID eventId = UUID.randomUUID();
        ConsumerRecord<String, String> record = record(eventId);
        when(objectMapper.readValue("payload", PaymentEventMessage.class))
                .thenReturn(message(eventId));
        when(repository.existsById(eventId)).thenReturn(true);

        consumer.consume(record);

        verify(repository, never()).save(org.mockito.ArgumentMatchers.any());
    }

    private ConsumerRecord<String, String> record(UUID eventId) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finpay.payment-events.v1", 2, 14L, "91", "payload");
        record.headers().add(
                "finpay-event-id", eventId.toString().getBytes(StandardCharsets.UTF_8));
        record.headers().add(
                "finpay-merchant-id", "7".getBytes(StandardCharsets.UTF_8));
        return record;
    }

    private PaymentEventMessage message(UUID eventId) {
        return new PaymentEventMessage(
                eventId,
                "payment.approved",
                Instant.parse("2026-09-24T10:00:00Z"),
                new EventData(new PaymentData(
                        91L,
                        new BigDecimal("49.90"),
                        "PEN",
                        "APPROVED")));
    }
}
