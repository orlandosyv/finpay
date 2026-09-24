package com.finpay.listener.dto;

import java.math.BigDecimal;
import java.time.Instant;
import java.util.UUID;

public record PaymentEventMessage(
        UUID id,
        String type,
        Instant createdAt,
        EventData data) {

    public record EventData(PaymentData payment) {
    }

    public record PaymentData(
            Long id,
            BigDecimal amount,
            String currency,
            String status) {
    }
}
