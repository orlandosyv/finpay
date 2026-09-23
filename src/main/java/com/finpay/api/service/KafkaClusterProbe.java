package com.finpay.api.service;

import java.util.Map;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.apache.kafka.clients.admin.Admin;
import org.apache.kafka.clients.admin.TopicDescription;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaAdmin;
import org.springframework.stereotype.Component;

@Component
public class KafkaClusterProbe {

    private static final long TIMEOUT_SECONDS = 3;

    private final KafkaAdmin kafkaAdmin;
    private final boolean enabled;

    public KafkaClusterProbe(
            KafkaAdmin kafkaAdmin,
            @Value("${finpay.kafka.enabled}") boolean enabled) {
        this.kafkaAdmin = kafkaAdmin;
        this.enabled = enabled;
    }

    public KafkaProbeResult inspect(String topicName) {
        if (!enabled) {
            return KafkaProbeResult.disabled();
        }

        try (Admin admin = Admin.create(kafkaAdmin.getConfigurationProperties())) {
            String clusterId = admin.describeCluster()
                    .clusterId()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            Map<String, TopicDescription> topics = admin.describeTopics(Set.of(topicName))
                    .allTopicNames()
                    .get(TIMEOUT_SECONDS, TimeUnit.SECONDS);
            TopicDescription topic = topics.get(topicName);
            int partitions = topic == null ? 0 : topic.partitions().size();
            return KafkaProbeResult.connected(clusterId, partitions);
        } catch (Exception exception) {
            return KafkaProbeResult.disconnected();
        }
    }

    public record KafkaProbeResult(
            String status,
            boolean topicAvailable,
            int partitions,
            String clusterId) {

        static KafkaProbeResult connected(String clusterId, int partitions) {
            return new KafkaProbeResult("CONNECTED", partitions > 0, partitions, clusterId);
        }

        static KafkaProbeResult disconnected() {
            return new KafkaProbeResult("DISCONNECTED", false, 0, null);
        }

        static KafkaProbeResult disabled() {
            return new KafkaProbeResult("DISABLED", false, 0, null);
        }
    }
}
