package com.finpay.api.service;

import static org.assertj.core.api.Assertions.assertThat;
import static org.mockito.Mockito.when;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;

import com.finpay.api.context.CurrentMerchantProvider;
import com.finpay.api.dto.InfrastructureStatusResponse;
import com.finpay.api.model.KafkaPublicationStatus;
import com.finpay.api.repository.KafkaPublicationRepository;
import com.finpay.api.repository.WebhookEndpointRepository;
import com.finpay.api.service.KafkaClusterProbe.KafkaProbeResult;

@ExtendWith(MockitoExtension.class)
class InfrastructureStatusServiceTest {

    @Mock
    private CurrentMerchantProvider currentMerchantProvider;

    @Mock
    private WebhookEndpointRepository webhookEndpointRepository;

    @Mock
    private KafkaPublicationRepository kafkaPublicationRepository;

    @Mock
    private KafkaClusterProbe kafkaClusterProbe;

    private InfrastructureStatusService service;

    @BeforeEach
    void setUp() {
        service = new InfrastructureStatusService(
                currentMerchantProvider,
                webhookEndpointRepository,
                kafkaPublicationRepository,
                kafkaClusterProbe,
                "finpay.payment-events.v1",
                true);
    }

    @Test
    void reportsActiveKafkaPublicationAndTenantMetrics() {
        when(currentMerchantProvider.getCurrentMerchantId()).thenReturn(42L);
        when(webhookEndpointRepository.countByMerchantIdAndActiveTrue(42L)).thenReturn(2L);
        when(kafkaPublicationRepository.countByMerchant_Id(42L)).thenReturn(8L);
        when(kafkaPublicationRepository.countByMerchant_IdAndStatus(
                42L, KafkaPublicationStatus.PUBLISHED)).thenReturn(6L);
        when(kafkaPublicationRepository.countByMerchant_IdAndStatus(
                42L, KafkaPublicationStatus.PENDING)).thenReturn(2L);
        when(kafkaClusterProbe.inspect("finpay.payment-events.v1"))
                .thenReturn(KafkaProbeResult.connected("cluster-1", 3));

        InfrastructureStatusResponse response = service.getStatus();

        assertThat(response.kafka().status()).isEqualTo("CONNECTED");
        assertThat(response.kafka().topicAvailable()).isTrue();
        assertThat(response.kafka().partitions()).isEqualTo(3);
        assertThat(response.kafka().publishingEnabled()).isTrue();
        assertThat(response.kafka().totalEvents()).isEqualTo(8);
        assertThat(response.kafka().publishedEvents()).isEqualTo(6);
        assertThat(response.kafka().pendingEvents()).isEqualTo(2);
        assertThat(response.webhooks().activeEndpoints()).isEqualTo(2);
        assertThat(response.pipeline().get(3).state()).isEqualTo("ACTIVE");
    }

    @Test
    void distinguishesUnavailableKafkaFromAnUnconfiguredWebhook() {
        when(currentMerchantProvider.getCurrentMerchantId()).thenReturn(42L);
        when(webhookEndpointRepository.countByMerchantIdAndActiveTrue(42L)).thenReturn(0L);
        when(kafkaClusterProbe.inspect("finpay.payment-events.v1"))
                .thenReturn(KafkaProbeResult.disconnected());

        InfrastructureStatusResponse response = service.getStatus();

        assertThat(response.kafka().status()).isEqualTo("DISCONNECTED");
        assertThat(response.webhooks().status()).isEqualTo("NOT_CONFIGURED");
        assertThat(response.pipeline().get(3).state()).isEqualTo("DISCONNECTED");
    }
}
