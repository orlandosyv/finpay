package com.finpay.api.repository;

import java.util.List;
import java.util.Optional;

import org.springframework.data.jpa.repository.JpaRepository;

import com.finpay.api.model.WebhookEndpoint;

public interface WebhookEndpointRepository extends JpaRepository<WebhookEndpoint, Long> {

    List<WebhookEndpoint> findAllByMerchantIdAndActiveTrueOrderByIdAsc(Long merchantId);

    Optional<WebhookEndpoint> findByIdAndMerchantIdAndActiveTrue(Long id, Long merchantId);

    long countByMerchantIdAndActiveTrue(Long merchantId);
}
