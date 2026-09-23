package com.finpay.api.config;

import static org.assertj.core.api.Assertions.assertThat;

import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicDescription;
import org.junit.jupiter.api.Test;
import org.testcontainers.junit.jupiter.Container;
import org.testcontainers.junit.jupiter.Testcontainers;
import org.testcontainers.kafka.KafkaContainer;

@Testcontainers(disabledWithoutDocker = true)
class KafkaInfrastructureIntegrationTest {

    private static final String TOPIC_NAME = "finpay.payment-events.v1";

    @Container
    private static final KafkaContainer KAFKA = new KafkaContainer("apache/kafka:4.3.1");

    @Test
    void createsTheVersionedPaymentEventsTopicWithThreePartitions() throws Exception {
        NewTopic topic = new KafkaTopicConfiguration().paymentEventsTopic(TOPIC_NAME);
        Properties properties = new Properties();
        properties.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA.getBootstrapServers());

        try (Admin admin = Admin.create(properties)) {
            admin.createTopics(List.of(topic)).all().get();
            Map<String, TopicDescription> descriptions = admin
                    .describeTopics(List.of(TOPIC_NAME))
                    .allTopicNames()
                    .get(10, TimeUnit.SECONDS);

            assertThat(descriptions).containsKey(TOPIC_NAME);
            assertThat(descriptions.get(TOPIC_NAME).partitions()).hasSize(3);
        }
    }
}
