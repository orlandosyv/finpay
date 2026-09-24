package com.finpay.api.service;

import java.net.URI;
import java.net.URLEncoder;
import java.net.http.HttpClient;
import java.net.http.HttpRequest;
import java.net.http.HttpResponse;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.time.Instant;
import java.util.List;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;

import com.finpay.api.dto.InfrastructureStatusResponse.KafkaConsumerStatus;
import com.finpay.api.dto.InfrastructureStatusResponse.KafkaDeadLetterStatus;

import tools.jackson.databind.ObjectMapper;

@Component
public class KafkaListenerProbe {

    private static final Duration TIMEOUT = Duration.ofSeconds(3);

    private final HttpClient httpClient;
    private final ObjectMapper objectMapper;
    private final String listenerUrl;

    public KafkaListenerProbe(
            ObjectMapper objectMapper,
            @Value("${finpay.kafka.listener-url:}") String listenerUrl) {
        this.objectMapper = objectMapper;
        this.listenerUrl = listenerUrl == null ? "" : listenerUrl.stripTrailing();
        this.httpClient = HttpClient.newBuilder()
                .connectTimeout(TIMEOUT)
                .build();
    }

    public KafkaConsumerStatus inspect(Long merchantId) {
        if (listenerUrl.isBlank()) {
            return unavailable("DISABLED", "The independent Kafka listener is not configured.");
        }

        try {
            String encodedMerchantId = URLEncoder.encode(
                    merchantId.toString(), StandardCharsets.UTF_8);
            HttpRequest request = HttpRequest.newBuilder()
                    .uri(URI.create(listenerUrl + "/internal/kafka/status?merchantId="
                            + encodedMerchantId))
                    .timeout(TIMEOUT)
                    .GET()
                    .build();
            HttpResponse<String> response = httpClient.send(
                    request, HttpResponse.BodyHandlers.ofString());
            if (response.statusCode() != 200) {
                return unavailable(
                        "UNAVAILABLE",
                        "The listener status endpoint returned HTTP " + response.statusCode() + ".");
            }
            return objectMapper.readValue(response.body(), KafkaConsumerStatus.class);
        } catch (InterruptedException exception) {
            Thread.currentThread().interrupt();
            return unavailable("UNAVAILABLE", "The listener status check was interrupted.");
        } catch (Exception exception) {
            return unavailable(
                    "UNAVAILABLE",
                    "The independent listener is stopped or cannot be reached.");
        }
    }

    private KafkaConsumerStatus unavailable(String status, String hint) {
        return new KafkaConsumerStatus(
                Instant.now(),
                status,
                "finpay-kafka-listener",
                null,
                "finpay-audit-v1",
                0,
                0,
                -1,
                null,
                List.of(),
                new KafkaDeadLetterStatus(
                        "finpay.payment-events.v1.DLT",
                        3,
                        2000,
                        0,
                        List.of()),
                hint);
    }
}
