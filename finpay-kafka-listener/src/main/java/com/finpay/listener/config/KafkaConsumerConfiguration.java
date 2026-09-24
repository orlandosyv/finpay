package com.finpay.listener.config;

import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.kafka.listener.DefaultErrorHandler;
import org.springframework.util.backoff.FixedBackOff;

@Configuration
public class KafkaConsumerConfiguration {

    @Bean
    DefaultErrorHandler kafkaErrorHandler() {
        // Preserve the record and block only its partition until processing succeeds.
        return new DefaultErrorHandler(new FixedBackOff(2000L, Long.MAX_VALUE));
    }
}
