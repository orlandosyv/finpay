package com.finpay.listener.repository;

import java.util.List;
import java.util.UUID;

import org.springframework.data.jpa.repository.JpaRepository;

import com.finpay.listener.model.ConsumedPaymentEvent;

public interface ConsumedPaymentEventRepository
        extends JpaRepository<ConsumedPaymentEvent, UUID> {

    long countByMerchantId(Long merchantId);

    List<ConsumedPaymentEvent> findTop10ByMerchantIdOrderByConsumedAtDesc(Long merchantId);
}
