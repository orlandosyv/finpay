package com.finpay.listener.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.nio.ByteBuffer;
import java.nio.charset.StandardCharsets;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.finpay.listener.model.DeadLetterPaymentEvent;
import com.finpay.listener.repository.DeadLetterPaymentEventRepository;

@ExtendWith(MockitoExtension.class)
class DeadLetterEventConsumerTest {

    @Mock
    private DeadLetterPaymentEventRepository repository;

    private DeadLetterEventConsumer consumer;

    @BeforeEach
    void setUp() {
        consumer = new DeadLetterEventConsumer(repository);
    }

    @Test
    void storesFailureMetadataAndOriginalKafkaPosition() {
        UUID eventId = UUID.randomUUID();
        ConsumerRecord<String, String> record = record(eventId);

        consumer.consume(record);

        ArgumentCaptor<DeadLetterPaymentEvent> captor = ArgumentCaptor.forClass(
                DeadLetterPaymentEvent.class);
        verify(repository).save(captor.capture());
        DeadLetterPaymentEvent saved = captor.getValue();
        assertThat(saved.getEventId()).isEqualTo(eventId);
        assertThat(saved.getMerchantId()).isEqualTo(42L);
        assertThat(saved.getOriginalTopic()).isEqualTo("finpay.payment-events.v1");
        assertThat(saved.getOriginalPartition()).isEqualTo(2);
        assertThat(saved.getOriginalOffset()).isEqualTo(18L);
        assertThat(saved.getDltTopic()).isEqualTo("finpay.payment-events.v1.DLT");
        assertThat(saved.getDltOffset()).isEqualTo(4L);
        assertThat(saved.getExceptionMessage()).isEqualTo("Payment event payload is invalid");
    }

    @Test
    void ignoresAnAlreadyStoredDeadLetterPosition() {
        ConsumerRecord<String, String> record = record(UUID.randomUUID());
        when(repository.existsByDltTopicAndDltPartitionAndDltOffset(
                "finpay.payment-events.v1.DLT", 2, 4L)).thenReturn(true);

        consumer.consume(record);

        verify(repository, never()).save(org.mockito.ArgumentMatchers.any());
    }

    private ConsumerRecord<String, String> record(UUID eventId) {
        ConsumerRecord<String, String> record = new ConsumerRecord<>(
                "finpay.payment-events.v1.DLT", 2, 4L, "91", "not-json");
        record.headers().add("finpay-event-id", bytes(eventId.toString()));
        record.headers().add("finpay-merchant-id", bytes("42"));
        record.headers().add("kafka_dlt-original-topic", bytes("finpay.payment-events.v1"));
        record.headers().add("kafka_dlt-original-partition", ByteBuffer.allocate(4).putInt(2).array());
        record.headers().add("kafka_dlt-original-offset", ByteBuffer.allocate(8).putLong(18L).array());
        record.headers().add("kafka_dlt-exception-fqcn", bytes("java.lang.IllegalArgumentException"));
        record.headers().add("kafka_dlt-exception-message", bytes("Payment event payload is invalid"));
        return record;
    }

    private byte[] bytes(String value) {
        return value.getBytes(StandardCharsets.UTF_8);
    }
}
