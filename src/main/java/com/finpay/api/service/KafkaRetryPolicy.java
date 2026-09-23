package com.finpay.api.service;

import java.time.Duration;

import org.springframework.stereotype.Component;

@Component
public class KafkaRetryPolicy {

    private static final int MAX_ATTEMPTS = 5;

    public boolean hasAnotherAttempt(int completedAttempts) {
        return completedAttempts < MAX_ATTEMPTS;
    }

    public Duration delayAfterFailure(int completedAttempts) {
        return switch (completedAttempts) {
            case 1 -> Duration.ofSeconds(5);
            case 2 -> Duration.ofSeconds(15);
            case 3 -> Duration.ofMinutes(1);
            default -> Duration.ofMinutes(5);
        };
    }
}
