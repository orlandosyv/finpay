package com.finpay.api.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.math.BigDecimal;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.annotation.Import;
import org.springframework.test.context.DynamicPropertyRegistry;
import org.springframework.test.context.DynamicPropertySource;
import org.springframework.transaction.support.TransactionTemplate;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

import com.finpay.api.model.KafkaPublication;
import com.finpay.api.model.KafkaPublicationStatus;
import com.finpay.api.model.Merchant;
import com.finpay.api.model.Payment;
import com.finpay.api.model.PaymentStatus;
import com.finpay.api.model.WebhookEventType;
import com.finpay.api.repository.KafkaPublicationRepository;
import com.finpay.api.repository.MerchantRepository;
import com.finpay.api.repository.OutboxEventRepository;
import com.finpay.api.repository.PaymentRepository;
import com.finpay.api.service.KafkaOutboxPublisher;
import com.finpay.api.service.PaymentEventService;

@SpringBootTest(properties = {
        "finpay.kafka.enabled=true",
        "finpay.kafka.publisher-enabled=true",
        "finpay.kafka.scheduler-enabled=false",
        "finpay.webhook.scheduler-enabled=false"
})
@Import(TestcontainersConfiguration.class)
@Testcontainers(disabledWithoutDocker = true)
class KafkaPublicationIntegrationTest {

    private static final String TOPIC = "finpay.payment-events.v1";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @DynamicPropertySource
    static void kafkaProperties(DynamicPropertyRegistry registry) {
        registry.add("spring.kafka.bootstrap-servers", KAFKA::getBootstrapServers);
    }

    @Autowired
    private MerchantRepository merchantRepository;

    @Autowired
    private PaymentRepository paymentRepository;

    @Autowired
    private OutboxEventRepository outboxEventRepository;

    @Autowired
    private KafkaPublicationRepository kafkaPublicationRepository;

    @Autowired
    private PaymentEventService paymentEventService;

    @Autowired
    private KafkaOutboxPublisher publisher;

    @Autowired
    private TransactionTemplate transactionTemplate;

    @BeforeEach
    void cleanEventData() {
        kafkaPublicationRepository.deleteAll();
        outboxEventRepository.deleteAll();
        paymentRepository.deleteAll();
    }

    @Test
    void publishesOutboxEventToRealKafkaAndPersistsItsPosition() {
        Merchant merchant = merchantRepository.findAll().getFirst();
        Payment payment = transactionTemplate.execute(status -> {
            Payment savedPayment = paymentRepository.save(new Payment(
                    merchant,
                    new BigDecimal("49.90"),
                    "PEN",
                    PaymentStatus.PENDING));
            paymentEventService.record(savedPayment, WebhookEventType.PAYMENT_CREATED);
            return savedPayment;
        });

        int processed = publisher.publishPendingEvents();
        ConsumerRecord<String, String> record = consumeOne();
        KafkaPublication publication = kafkaPublicationRepository.findAll().getFirst();

        assertThat(processed).isOne();
        assertThat(record.key()).isEqualTo(payment.getId().toString());
        assertThat(record.value())
                .contains("\"type\":\"payment.created\"")
                .contains("\"id\":" + payment.getId());
        assertThat(header(record, "finpay-event-id"))
                .isEqualTo(publication.getEventId().toString());
        assertThat(header(record, "finpay-event-type")).isEqualTo("payment.created");
        assertThat(publication.getStatus()).isEqualTo(KafkaPublicationStatus.PUBLISHED);
        assertThat(publication.getPartition()).isEqualTo(record.partition());
        assertThat(publication.getOffset()).isEqualTo(record.offset());
        assertThat(kafkaPublicationRepository.countByMerchant_Id(merchant.getId())).isOne();
        assertThat(kafkaPublicationRepository.countByMerchant_IdAndStatus(
                merchant.getId(), KafkaPublicationStatus.PUBLISHED)).isOne();
        assertThat(kafkaPublicationRepository
                .findTop10ByMerchant_IdOrderByCreatedAtDesc(merchant.getId()))
                .extracting(KafkaPublication::getEventId)
                .containsExactly(publication.getEventId());
    }

    private ConsumerRecord<String, String> consumeOne() {
        Map<String, Object> properties = Map.of(
                ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers(),
                ConsumerConfig.GROUP_ID_CONFIG, "finpay-test-" + UUID.randomUUID(),
                ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest",
                ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class,
                ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(properties)) {
            consumer.subscribe(List.of(TOPIC));
            long deadline = System.nanoTime() + Duration.ofSeconds(15).toNanos();
            while (System.nanoTime() < deadline) {
                ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(500));
                if (!records.isEmpty()) {
                    return records.iterator().next();
                }
            }
        }
        throw new AssertionError("No payment event was consumed from Kafka");
    }

    private String header(ConsumerRecord<String, String> record, String name) {
        return new String(record.headers().lastHeader(name).value(), StandardCharsets.UTF_8);
    }
}
