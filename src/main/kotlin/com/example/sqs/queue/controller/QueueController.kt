package com.example.sqs.queue.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.domain.ErrorCode
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.service.QueueService
import com.example.sqs.tenant.service.TenantService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import jakarta.validation.Valid

@RestController
@RequestMapping("/api/v1/queues")
class QueueController(
    private val queueService: QueueService,
    private val tenantService: TenantService
) {
    
    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @Valid @RequestBody request: CreateQueueRequest
    ): ApiResponse<Queue> {
        val tenantId = tenantService.getTenantId(apiKey)
            ?: return ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)
        
        try {
            val queue = queueService.createQueue(tenantId, request.queueName, request.attributes)
            return ApiResponse.success(queue)
        } catch (e: IllegalArgumentException) {
            return ApiResponse.error(ErrorCode.QUEUE_ALREADY_EXISTS.name, e.message ?: "Queue creation failed")
        }
    }
}
