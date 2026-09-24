package com.finpay.listener.repository;

import java.util.List;

import org.springframework.data.jpa.repository.JpaRepository;

import com.finpay.listener.model.DeadLetterPaymentEvent;

public interface DeadLetterPaymentEventRepository
        extends JpaRepository<DeadLetterPaymentEvent, Long> {

    long countByMerchantId(Long merchantId);

    List<DeadLetterPaymentEvent> findTop10ByMerchantIdOrderByFailedAtDesc(Long merchantId);

    boolean existsByDltTopicAndDltPartitionAndDltOffset(
            String dltTopic,
            int dltPartition,
            long dltOffset);
}
