package com.finpay.listener.controller;

import java.time.Instant;
import java.util.Map;

import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import com.finpay.listener.dto.ListenerStatusResponse;
import com.finpay.listener.service.ListenerStatusService;

@RestController
@RequestMapping("/internal")
public class ListenerStatusController {

    private final ListenerStatusService statusService;

    public ListenerStatusController(ListenerStatusService statusService) {
        this.statusService = statusService;
    }

    @GetMapping("/health")
    public ResponseEntity<Map<String, Object>> health() {
        return ResponseEntity.ok(Map.of(
                "status", "UP",
                "application", "finpay-kafka-listener",
                "checkedAt", Instant.now()));
    }

    @GetMapping("/kafka/status")
    public ListenerStatusResponse kafkaStatus(@RequestParam Long merchantId) {
        return statusService.getStatus(merchantId);
    }
}
