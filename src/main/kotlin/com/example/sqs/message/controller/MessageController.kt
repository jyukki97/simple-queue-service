package com.example.sqs.message.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.exception.*
import com.example.sqs.message.domain.*
import com.example.sqs.message.service.MessageService
import com.example.sqs.tenant.service.TenantService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import org.slf4j.LoggerFactory

@RestController
@RequestMapping("/api/v1/queues/{queueName}/messages")
class MessageController(
    private val messageService: MessageService,
    private val tenantService: TenantService
) {
    private val logger = LoggerFactory.getLogger(MessageController::class.java)

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessage(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: SendMessageRequest
    ): Mono<ApiResponse<SendMessageResponse>> {
        logger.info("메시지 전송 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageService.sendMessage(tenantId, queueName, request)
                    .map { response -> ApiResponse.success(response) }
            }
    }

    @GetMapping
    fun receiveMessages(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "1") maxNumberOfMessages: Int,
        @RequestParam(defaultValue = "30") visibilityTimeoutSeconds: Int,
        @RequestParam(defaultValue = "0") waitTimeSeconds: Int
    ): Mono<ApiResponse<ReceiveMessageResponse>> {
        logger.info("메시지 수신 요청: $queueName, maxMessages: $maxNumberOfMessages")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                val request = ReceiveMessageRequest(maxNumberOfMessages, visibilityTimeoutSeconds, waitTimeSeconds)
                messageService.receiveMessages(tenantId, queueName, request)
                    .collectList()
                    .map { messages -> ApiResponse.success(ReceiveMessageResponse(messages)) }
            }
    }

    @DeleteMapping("/{receiptHandle}")
    fun deleteMessage(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @PathVariable receiptHandle: String
    ): Mono<ApiResponse<String>> {
        logger.info("메시지 삭제 요청: $queueName, receiptHandle: $receiptHandle")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageService.deleteMessage(tenantId, queueName, receiptHandle)
                    .then(Mono.just(ApiResponse.success("메시지가 성공적으로 삭제되었습니다")))
            }
    }

    @DeleteMapping
    fun purgeQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        logger.info("큐 메시지 전체 삭제 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageService.purgeQueue(tenantId, queueName)
                    .then(Mono.just(ApiResponse.success("큐의 모든 메시지가 삭제되었습니다")))
            }
    }

    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessageBatch(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: SendMessageBatchRequest
    ): Mono<ApiResponse<SendMessageBatchResponse>> {
        logger.info("메시지 일괄 전송 요청: $queueName, 메시지 개수: ${request.messages.size}")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageService.sendMessageBatch(tenantId, queueName, request)
                    .map { response -> ApiResponse.success(response) }
            }
    }

    @DeleteMapping("/batch")
    fun deleteMessageBatch(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: DeleteMessageBatchRequest
    ): Mono<ApiResponse<DeleteMessageBatchResponse>> {
        logger.info("메시지 일괄 삭제 요청: $queueName, 메시지 개수: ${request.receiptHandles.size}")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageService.deleteMessageBatch(tenantId, queueName, request)
                    .map { response -> ApiResponse.success(response) }
            }
    }
}
