package com.finpay.listener.service;

import java.time.Instant;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.ListOffsetsResult.ListOffsetsResultInfo;
import org.apache.kafka.clients.admin.OffsetSpec;
import org.apache.kafka.clients.admin.TopicDescription;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.config.KafkaListenerEndpointRegistry;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.kafka.listener.MessageListenerContainer;
import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.finpay.listener.dto.ListenerStatusResponse;
import com.finpay.listener.dto.ListenerStatusResponse.ConsumedEventResponse;
import com.finpay.listener.dto.ListenerStatusResponse.DeadLetterEventResponse;
import com.finpay.listener.dto.ListenerStatusResponse.DeadLetterStatus;
import com.finpay.listener.model.ConsumedPaymentEvent;
import com.finpay.listener.model.DeadLetterPaymentEvent;
import com.finpay.listener.repository.ConsumedPaymentEventRepository;
import com.finpay.listener.repository.DeadLetterPaymentEventRepository;

@Service
public class ListenerStatusService {

    private static final long TIMEOUT_SECONDS = 3;
    private static final String LISTENER_ID = "finpay-payment-audit-listener";

    private final ConsumedPaymentEventRepository repository;
    private final DeadLetterPaymentEventRepository deadLetterRepository;
    private final KafkaListenerEndpointRegistry registry;
    private final KafkaAdmin kafkaAdmin;
    private final String topic;
    private final String consumerGroup;
    private final int concurrency;
    private final String dltTopic;
    private final int maxRetries;
    private final long retryIntervalMs;

    public ListenerStatusService(
            ConsumedPaymentEventRepository repository,
            DeadLetterPaymentEventRepository deadLetterRepository,
            KafkaListenerEndpointRegistry registry,
            KafkaAdmin kafkaAdmin,
            @Value("${finpay.kafka.topic}") String topic,
            @Value("${finpay.kafka.consumer-group}") String consumerGroup,
            @Value("${finpay.kafka.concurrency:3}") int concurrency,
            @Value("${finpay.kafka.dlt-topic}") String dltTopic,
            @Value("${finpay.kafka.max-retries:3}") int maxRetries,
            @Value("${finpay.kafka.retry-interval-ms:2000}") long retryIntervalMs) {
        this.repository = repository;
        this.deadLetterRepository = deadLetterRepository;
        this.registry = registry;
        this.kafkaAdmin = kafkaAdmin;
        this.topic = topic;
        this.consumerGroup = consumerGroup;
        this.concurrency = concurrency;
        this.dltTopic = dltTopic;
        this.maxRetries = maxRetries;
        this.retryIntervalMs = retryIntervalMs;
    }

    @Transactional(readOnly = true)
    public ListenerStatusResponse getStatus(Long merchantId) {
        List<ConsumedPaymentEvent> events = repository
                .findTop10ByMerchantIdOrderByConsumedAtDesc(merchantId);
        long consumedEvents = repository.countByMerchantId(merchantId);
        List<DeadLetterPaymentEvent> deadLetters = deadLetterRepository
                .findTop10ByMerchantIdOrderByFailedAtDesc(merchantId);
        long deadLetterEvents = deadLetterRepository.countByMerchantId(merchantId);
        BrokerStatus broker = inspectBroker();
        MessageListenerContainer container = registry.getListenerContainer(LISTENER_ID);
        boolean listenerRunning = container != null && container.isRunning();
        String status = !broker.connected()
                ? "DISCONNECTED"
                : listenerRunning ? "RUNNING" : "STOPPED";

        return new ListenerStatusResponse(
                Instant.now(),
                status,
                "finpay-kafka-listener",
                topic,
                consumerGroup,
                concurrency,
                consumedEvents,
                broker.lag(),
                events.isEmpty() ? null : events.getFirst().getConsumedAt(),
                events.stream().map(this::toResponse).toList(),
                new DeadLetterStatus(
                        dltTopic,
                        maxRetries,
                        retryIntervalMs,
                        deadLetterEvents,
                        deadLetters.stream().map(this::toDeadLetterResponse).toList()),
                hint(status, broker.lag()));
    }

    private BrokerStatus inspectBroker() {
        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            TopicDescription description = admin.describeTopics(Set.of(topic))
                    .allTopicNames()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS)
                    .get(topic);
            Map<TopicPartition, OffsetAndMetadata> committed = admin
                    .listConsumerGroupOffsets(consumerGroup)
                    .partitionsToOffsetAndMetadata()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<TopicPartition, OffsetSpec> latestRequests = new HashMap<>();
            Map<TopicPartition, OffsetSpec> earliestRequests = new HashMap<>();
            description.partitions().forEach(partition -> latestRequests.put(
                    new TopicPartition(topic, partition.partition()),
                    OffsetSpec.latest()));
            Map<TopicPartition, ListOffsetsResultInfo> endOffsets = admin
                    .listOffsets(latestRequests)
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            description.partitions().forEach(partition -> earliestRequests.put(
                    new TopicPartition(topic, partition.partition()),
                    OffsetSpec.earliest()));
            Map<TopicPartition, ListOffsetsResultInfo> beginningOffsets = admin
                    .listOffsets(earliestRequests)
                    .all()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);

            long lag = endOffsets.entrySet().stream()
                    .mapToLong(entry -> {
                        OffsetAndMetadata current = committed.get(entry.getKey());
                        long committedOffset = current == null
                                ? beginningOffsets.get(entry.getKey()).offset()
                                : current.offset();
                        return Math.max(0, entry.getValue().offset() - committedOffset);
                    })
                    .sum();
            return new BrokerStatus(true, lag);
        } catch (Exception exception) {
            return new BrokerStatus(false, -1);
        }
    }

    private ConsumedEventResponse toResponse(ConsumedPaymentEvent event) {
        return new ConsumedEventResponse(
                event.getEventId().toString(),
                event.getPaymentId(),
                event.getEventType(),
                event.getPaymentStatus(),
                event.getPartition(),
                event.getOffset(),
                event.getOccurredAt(),
                event.getConsumedAt());
    }

    private DeadLetterEventResponse toDeadLetterResponse(DeadLetterPaymentEvent event) {
        return new DeadLetterEventResponse(
                event.getEventId() == null ? null : event.getEventId().toString(),
                event.getOriginalTopic(),
                event.getOriginalPartition(),
                event.getOriginalOffset(),
                event.getExceptionClass(),
                event.getExceptionMessage(),
                event.getFailedAt());
    }

    private String hint(String status, long lag) {
        if ("RUNNING".equals(status)) {
            return lag == 0
                    ? "The independent listener is running and its consumer group is caught up."
                    : "The listener is running and has " + lag
                            + " Kafka record(s) left to process.";
        }
        if ("STOPPED".equals(status)) {
            return "Kafka is reachable, but the listener container is not running.";
        }
        return "The listener cannot inspect Kafka. Published records remain durable in the topic.";
    }

    private record BrokerStatus(boolean connected, long lag) {
    }
}
