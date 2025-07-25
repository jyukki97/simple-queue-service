package com.example.sqs.message.controller

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.domain.SendMessageResponse
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.ReceiveMessageResponse
import com.example.sqs.message.domain.SendMessageBatchRequest
import com.example.sqs.message.domain.SendMessageBatchResponse
import com.example.sqs.message.domain.DeleteMessageBatchRequest
import com.example.sqs.message.domain.DeleteMessageBatchResponse
import com.example.sqs.message.domain.PeekMessageResponse
import com.example.sqs.message.service.MessageService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.RestController
import org.springframework.web.bind.annotation.RequestMapping
import org.springframework.web.bind.annotation.PostMapping
import org.springframework.web.bind.annotation.GetMapping
import org.springframework.web.bind.annotation.DeleteMapping
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.PathVariable
import org.springframework.web.bind.annotation.RequestBody
import org.springframework.web.bind.annotation.RequestParam
import reactor.core.publisher.Mono
import org.slf4j.LoggerFactory

@RestController
@RequestMapping("/api/v1/queues/{queueName}/messages")
class MessageController(
    private val messageService: MessageService
) {
    private val logger = LoggerFactory.getLogger(MessageController::class.java)

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessage(
        @PathVariable queueName: String,
        @RequestBody request: SendMessageRequest
    ): Mono<ApiResponse<SendMessageResponse>> {
        logger.info("메시지 전송 요청: $queueName")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.sendMessage(tenantId, queueName, request)
            .map { response -> ApiResponse.success(response) }
    }

    @GetMapping
    fun receiveMessages(
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "1") maxNumberOfMessages: Int,
        @RequestParam(defaultValue = "30") visibilityTimeoutSeconds: Int,
        @RequestParam(defaultValue = "0") waitTimeSeconds: Int,
        @RequestParam(required = false) messageGroupId: String?
    ): Mono<ApiResponse<ReceiveMessageResponse>> {
        logger.info("메시지 수신 요청: $queueName, maxMessages: $maxNumberOfMessages, messageGroupId: $messageGroupId")
        val tenantId = AuthContext.getCurrentTenantId()
        val request = ReceiveMessageRequest(
            maxNumberOfMessages = maxNumberOfMessages, 
            visibilityTimeoutSeconds = visibilityTimeoutSeconds, 
            waitTimeSeconds = waitTimeSeconds,
            messageGroupId = messageGroupId
        )
        return messageService.receiveMessages(tenantId, queueName, request)
            .collectList()
            .map { messages -> ApiResponse.success(ReceiveMessageResponse(messages)) }
    }

    @DeleteMapping("/{receiptHandle}")
    fun deleteMessage(
        @PathVariable queueName: String,
        @PathVariable receiptHandle: String
    ): Mono<ApiResponse<String>> {
        logger.info("메시지 삭제 요청: $queueName, receiptHandle: $receiptHandle")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.deleteMessage(tenantId, queueName, receiptHandle)
            .then(Mono.just(ApiResponse.success("메시지가 성공적으로 삭제되었습니다")))
    }

    @DeleteMapping
    fun purgeQueue(
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        logger.info("큐 메시지 전체 삭제 요청: $queueName")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.purgeQueue(tenantId, queueName)
            .then(Mono.just(ApiResponse.success("큐의 모든 메시지가 삭제되었습니다")))
    }

    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessageBatch(
        @PathVariable queueName: String,
        @RequestBody request: SendMessageBatchRequest
    ): Mono<ApiResponse<SendMessageBatchResponse>> {
        logger.info("메시지 일괄 전송 요청: $queueName, 메시지 개수: ${request.messages.size}")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.sendMessageBatch(tenantId, queueName, request)
            .map { response -> ApiResponse.success(response) }
    }

    @DeleteMapping("/batch")
    fun deleteMessageBatch(
        @PathVariable queueName: String,
        @RequestBody request: DeleteMessageBatchRequest
    ): Mono<ApiResponse<DeleteMessageBatchResponse>> {
        logger.info("메시지 일괄 삭제 요청: $queueName, 메시지 개수: ${request.receiptHandles.size}")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.deleteMessageBatch(tenantId, queueName, request)
            .map { response -> ApiResponse.success(response) }
    }

    @GetMapping("/peek")
    fun peekMessages(
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "10") maxMessages: Int
    ): Mono<ApiResponse<PeekMessageResponse>> {
        logger.info("메시지 미리보기 요청: $queueName, maxMessages: $maxMessages")
        val tenantId = AuthContext.getCurrentTenantId()
        return messageService.peekMessages(tenantId, queueName, maxMessages)
            .collectList()
            .map { messages -> ApiResponse.success(PeekMessageResponse(messages)) }
    }

}
