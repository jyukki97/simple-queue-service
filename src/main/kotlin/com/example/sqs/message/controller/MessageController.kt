package com.example.sqs.message.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.domain.ErrorCode
import com.example.sqs.message.domain.MessagePreview
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.domain.SendMessageResponse
import com.example.sqs.message.domain.SendMessageBatchRequest
import com.example.sqs.message.domain.SendMessageBatchResponse
import com.example.sqs.message.domain.DeleteMessageBatchRequest
import com.example.sqs.message.domain.DeleteMessageBatchResponse
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.MessageResponse
import com.example.sqs.message.service.MessageService
import com.example.sqs.tenant.service.TenantService
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

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
        logger.info("메시지 전송 요청: queue=$queueName, body=${request.messageBody.take(100)}")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.sendMessage(tenantId, queueName, request)
                    .map { response ->
                        logger.info("메시지 전송 성공: messageId=${response.messageId}")
                        ApiResponse.success(response)
                    }
                    .onErrorResume { e ->
                        logger.error("메시지 전송 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, e.message ?: "잘못된 파라미터입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 전송 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @GetMapping
    fun receiveMessages(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "1") maxNumberOfMessages: Int,
        @RequestParam(required = false) visibilityTimeoutSeconds: Int?,
        @RequestParam(defaultValue = "0") waitTimeSeconds: Int
    ): Mono<ApiResponse<List<MessageResponse>>> {
        logger.info("메시지 수신 요청: queue=$queueName, maxMessages=$maxNumberOfMessages")
        
        val request = ReceiveMessageRequest(
            maxNumberOfMessages = maxNumberOfMessages,
            visibilityTimeoutSeconds = visibilityTimeoutSeconds,
            waitTimeSeconds = waitTimeSeconds
        )
        
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.receiveMessages(tenantId, queueName, request)
                    .collectList()
                    .map { messages ->
                        logger.info("메시지 수신 성공: 수신된 메시지 수=${messages.size}")
                        ApiResponse.success(messages)
                    }
                    .onErrorResume { e ->
                        logger.error("메시지 수신 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, e.message ?: "잘못된 파라미터입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 수신 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @DeleteMapping("/{receiptHandle}")
    fun deleteMessage(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @PathVariable receiptHandle: String
    ): Mono<ApiResponse<String>> {
        logger.info("메시지 삭제 요청: queue=$queueName, receiptHandle=$receiptHandle")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.deleteMessage(tenantId, queueName, receiptHandle)
                    .then(Mono.just(ApiResponse.success("메시지가 성공적으로 삭제되었습니다")))
                    .onErrorResume { e ->
                        logger.error("메시지 삭제 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_RECEIPT_HANDLE.name, e.message ?: "잘못된 receipt handle입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 삭제 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @DeleteMapping
    fun purgeQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        logger.info("큐 메시지 전체 삭제 요청: queue=$queueName")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.purgeQueue(tenantId, queueName)
                    .then(Mono.just(ApiResponse.success("큐의 모든 메시지가 삭제되었습니다: $queueName")))
                    .onErrorResume { e ->
                        logger.error("큐 메시지 전체 삭제 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 메시지 전체 삭제 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @PostMapping("/batch")
    @ResponseStatus(HttpStatus.CREATED)
    fun sendMessageBatch(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: SendMessageBatchRequest
    ): Mono<ApiResponse<SendMessageBatchResponse>> {
        logger.info("메시지 일괄 전송 요청: queue=$queueName, count=${request.messages.size}")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.sendMessageBatch(tenantId, queueName, request)
                    .map { response ->
                        logger.info("메시지 일괄 전송 완료: 성공=${response.successful.size}, 실패=${response.failed.size}")
                        ApiResponse.success(response)
                    }
                    .onErrorResume { e ->
                        logger.error("메시지 일괄 전송 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, e.message ?: "잘못된 파라미터입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 일괄 전송 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @PostMapping("/delete-batch")
    fun deleteMessageBatch(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: DeleteMessageBatchRequest
    ): Mono<ApiResponse<DeleteMessageBatchResponse>> {
        logger.info("메시지 일괄 삭제 요청: queue=$queueName, count=${request.receiptHandles.size}")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.deleteMessageBatch(tenantId, queueName, request)
                    .map { response ->
                        logger.info("메시지 일괄 삭제 완료: 성공=${response.successful.size}, 실패=${response.failed.size}")
                        ApiResponse.success(response)
                    }
                    .onErrorResume { e ->
                        logger.error("메시지 일괄 삭제 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, e.message ?: "잘못된 파라미터입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 일괄 삭제 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @GetMapping("/peek")
    fun peekMessages(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestParam(defaultValue = "10") maxMessages: Int
    ): Mono<ApiResponse<List<MessagePreview>>> {
        logger.info("메시지 미리보기 요청: queue=$queueName, maxMessages=$maxMessages")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                messageService.peekMessages(tenantId, queueName, maxMessages)
                    .collectList()
                    .map { messages ->
                        logger.info("메시지 미리보기 완료: queue=$queueName, count=${messages.size}")
                        ApiResponse.success(messages)
                    }
                    .onErrorResume { e ->
                        logger.error("메시지 미리보기 중 오류 발생", e)
                        when (e) {
                            is IllegalArgumentException -> Mono.just(
                                if (e.message?.contains("큐") == true) {
                                    ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다")
                                } else {
                                    ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, e.message ?: "잘못된 파라미터입니다")
                                }
                            )
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "메시지 미리보기 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }
}
