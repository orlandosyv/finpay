package com.finpay.api.service;

import java.time.Instant;
import java.util.UUID;

import org.springframework.stereotype.Service;
import org.springframework.transaction.annotation.Transactional;

import com.finpay.api.model.KafkaPublication;
import com.finpay.api.repository.KafkaPublicationRepository;

@Service
public class KafkaPublicationStateService {

    private final KafkaPublicationRepository repository;
    private final KafkaRetryPolicy retryPolicy;

    public KafkaPublicationStateService(
            KafkaPublicationRepository repository,
            KafkaRetryPolicy retryPolicy) {
        this.repository = repository;
        this.retryPolicy = retryPolicy;
    }

    @Transactional
    public void markPublished(UUID eventId, int partition, long offset) {
        KafkaPublication publication = repository.findById(eventId).orElseThrow();
        publication.markPublished(Instant.now(), partition, offset);
    }

    @Transactional
    public void retryOrFail(UUID eventId, String error) {
        KafkaPublication publication = repository.findById(eventId).orElseThrow();
        int completedAttempts = publication.getAttempts() + 1;
        String safeError = truncate(error);
        if (retryPolicy.hasAnotherAttempt(completedAttempts)) {
            publication.scheduleRetry(
                    Instant.now().plus(retryPolicy.delayAfterFailure(completedAttempts)),
                    safeError);
        } else {
            publication.markFailed(safeError);
        }
    }

    private String truncate(String message) {
        if (message == null || message.length() <= 1000) {
            return message;
        }
        return message.substring(0, 1000);
    }
}
