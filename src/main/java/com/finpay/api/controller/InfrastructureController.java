package com.finpay.api.controller;

import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RestController;

import com.finpay.api.dto.ApiError;
import com.finpay.api.dto.InfrastructureStatusResponse;
import com.finpay.api.service.InfrastructureStatusService;

import io.swagger.v3.oas.annotations.Operation;
import io.swagger.v3.oas.annotations.media.Content;
import io.swagger.v3.oas.annotations.media.Schema;
import io.swagger.v3.oas.annotations.responses.ApiResponse;
import io.swagger.v3.oas.annotations.responses.ApiResponses;
import io.swagger.v3.oas.annotations.security.SecurityRequirement;
import io.swagger.v3.oas.annotations.tags.Tag;

@RestController
@RequestMapping("/api/merchant/infrastructure")
@Tag(name = "Infrastructure lab", description = "Observe Kafka and webhook infrastructure")
@SecurityRequirement(name = "bearerAuth")
@ApiResponses({
        @ApiResponse(responseCode = "401", description = "Authentication is required or the access token is invalid", content = @Content(schema = @Schema(implementation = ApiError.class))),
        @ApiResponse(responseCode = "403", description = "Only MERCHANT_ADMIN can inspect infrastructure", content = @Content(schema = @Schema(implementation = ApiError.class)))
})
public class InfrastructureController {

    private final InfrastructureStatusService infrastructureStatusService;

    public InfrastructureController(InfrastructureStatusService infrastructureStatusService) {
        this.infrastructureStatusService = infrastructureStatusService;
    }

    @GetMapping
    @Operation(summary = "Inspect Kafka, webhooks and the payment event pipeline")
    @ApiResponse(responseCode = "200", description = "Current infrastructure status")
    public InfrastructureStatusResponse getStatus() {
        return infrastructureStatusService.getStatus();
    }
}
