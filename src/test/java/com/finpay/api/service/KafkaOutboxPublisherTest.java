package com.finpay.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.time.Instant;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import com.finpay.api.model.KafkaPublication;
import com.finpay.api.model.KafkaPublicationStatus;
import com.finpay.api.model.OutboxEvent;
import com.finpay.api.model.WebhookEventType;
import com.finpay.api.repository.KafkaPublicationRepository;

@ExtendWith(MockitoExtension.class)
class KafkaOutboxPublisherTest {

    @Mock
    private KafkaPublicationRepository repository;

    @Mock
    private KafkaTemplate<String, String> kafkaTemplate;

    @Mock
    private KafkaPublicationStateService stateService;

    private KafkaOutboxPublisher publisher;

    @BeforeEach
    void setUp() {
        publisher = new KafkaOutboxPublisher(
                repository,
                kafkaTemplate,
                stateService,
                "finpay.payment-events.v1",
                1);
    }

    @Test
    void publishesPayloadKeyAndHeadersThenStoresKafkaPosition() throws Exception {
        UUID eventId = UUID.randomUUID();
        OutboxEvent event = mock(OutboxEvent.class);
        KafkaPublication publication = mock(KafkaPublication.class);
        when(publication.getEvent()).thenReturn(event);
        when(event.getId()).thenReturn(eventId);
        when(event.getAggregateId()).thenReturn(91L);
        when(event.getMerchantId()).thenReturn(7L);
        when(event.getEventType()).thenReturn(WebhookEventType.PAYMENT_CREATED);
        when(event.getPayload()).thenReturn("{\"id\":\"" + eventId + "\"}");
        when(event.getCreatedAt()).thenReturn(Instant.parse("2026-09-23T10:00:00Z"));
        when(repository.findTop20ByStatusAndAvailableAtLessThanEqualOrderByCreatedAtAsc(
                any(), any())).thenReturn(List.of(publication));

        RecordMetadata metadata = mock(RecordMetadata.class);
        when(metadata.partition()).thenReturn(2);
        when(metadata.offset()).thenReturn(14L);
        @SuppressWarnings("unchecked")
        SendResult<String, String> result = mock(SendResult.class);
        when(result.getRecordMetadata()).thenReturn(metadata);
        when(kafkaTemplate.send(any(ProducerRecord.class)))
                .thenReturn(CompletableFuture.completedFuture(result));

        int processed = publisher.publishPendingEvents();

        ArgumentCaptor<ProducerRecord<String, String>> recordCaptor = ArgumentCaptor.forClass(
                ProducerRecord.class);
        verify(kafkaTemplate).send(recordCaptor.capture());
        ProducerRecord<String, String> record = recordCaptor.getValue();
        assertThat(processed).isOne();
        assertThat(record.topic()).isEqualTo("finpay.payment-events.v1");
        assertThat(record.key()).isEqualTo("91");
        assertThat(record.value()).contains(eventId.toString());
        assertThat(new String(record.headers().lastHeader("finpay-event-id").value()))
                .isEqualTo(eventId.toString());
        verify(stateService).markPublished(eventId, 2, 14L);
    }
}
