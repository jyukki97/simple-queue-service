package com.example.sqs.queue.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.domain.ErrorCode
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.service.QueueService
import com.example.sqs.tenant.service.TenantService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import jakarta.validation.Valid
import org.slf4j.LoggerFactory

@RestController
@RequestMapping("/api/v1/queues")
class QueueController(
    private val queueService: QueueService,
    private val tenantService: TenantService
) {
    private val logger = LoggerFactory.getLogger(QueueController::class.java)
    
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestBody request: CreateQueueRequest
    ): Mono<ApiResponse<Queue>> {
        logger.info("Received create queue request for: ${request.queueName}")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                queueService.createQueue(tenantId, request.queueName, request.attributes)
                    .map { queue -> ApiResponse.success(queue) }
                    .onErrorResume { e ->
                        when (e) {
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_ALREADY_EXISTS.name, e.message ?: "Queue creation failed"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "Queue creation failed"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }
}
