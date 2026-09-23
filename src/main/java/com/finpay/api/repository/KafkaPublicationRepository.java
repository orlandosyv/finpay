package com.finpay.api.repository;

import java.time.Instant;
import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.EntityGraph;
import org.springframework.data.jpa.repository.JpaRepository;

import com.finpay.api.model.KafkaPublication;
import com.finpay.api.model.KafkaPublicationStatus;

public interface KafkaPublicationRepository extends JpaRepository<KafkaPublication, UUID> {

    @EntityGraph(attributePaths = "event")
    List<KafkaPublication> findTop20ByStatusAndAvailableAtLessThanEqualOrderByCreatedAtAsc(
            KafkaPublicationStatus status,
            Instant availableAt);

    long countByMerchant_Id(Long merchantId);

    long countByMerchant_IdAndStatus(Long merchantId, KafkaPublicationStatus status);

    @EntityGraph(attributePaths = "event")
    List<KafkaPublication> findTop10ByMerchant_IdOrderByCreatedAtDesc(Long merchantId);
}
