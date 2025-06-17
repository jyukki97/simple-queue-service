package com.example.sqs.queue.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.exception.*
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.UpdateQueueRequest
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.service.QueueService
import com.example.sqs.tenant.service.TenantService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
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
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.createQueue(tenantId, request.queueName, request.attributes)
                    .map { queue -> ApiResponse.success(queue) }
                    .onErrorMap { e ->
                        when (e) {
                            is IllegalArgumentException -> DuplicateResourceException(e.message ?: "Queue already exists")
                            else -> OperationFailedException(e.message ?: "Queue creation failed")
                        }
                    }
            }
    }

    @DeleteMapping("/{queueName}")
    fun deleteQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        logger.info("큐 삭제 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.deleteQueue(tenantId, queueName)
                    .then(Mono.just(ApiResponse.success("큐가 성공적으로 삭제되었습니다: $queueName")))
                    .onErrorMap { e ->
                        when (e) {
                            is QueueNotFoundException -> e
                            is IllegalArgumentException -> QueueNotFoundException(e.message ?: "큐를 찾을 수 없습니다")
                            else -> OperationFailedException(e.message ?: "큐 삭제 실패")
                        }
                    }
            }
    }

    @GetMapping
    fun listQueues(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestParam(defaultValue = "20") limit: Int,
        @RequestParam(defaultValue = "0") offset: Int,
        @RequestParam(required = false) namePattern: String?
    ): Mono<ApiResponse<List<Queue>>> {
        logger.info("큐 목록 조회 요청 - limit: $limit, offset: $offset, pattern: $namePattern")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.listQueues(tenantId, limit, offset, namePattern)
                    .collectList()
                    .map { queues -> ApiResponse.success(queues) }
                    .onErrorMap { e -> OperationFailedException(e.message ?: "큐 목록 조회 실패") }
            }
    }

    @GetMapping("/{queueName}/attributes")
    fun getQueueAttributes(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<Queue>> {
        logger.info("큐 속성 조회 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.getQueue(tenantId, queueName)
                    .map { queue -> ApiResponse.success(queue) }
                    .onErrorMap { e ->
                        when (e) {
                            is QueueNotFoundException -> e
                            is IllegalArgumentException -> QueueNotFoundException(e.message ?: "큐를 찾을 수 없습니다")
                            else -> OperationFailedException(e.message ?: "큐 속성 조회 실패")
                        }
                    }
            }
    }

    @PutMapping("/{queueName}/attributes")
    fun setQueueAttributes(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: UpdateQueueRequest
    ): Mono<ApiResponse<Queue>> {
        logger.info("큐 속성 수정 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.updateQueue(tenantId, queueName, request.attributes)
                    .map { queue -> ApiResponse.success(queue) }
                    .onErrorMap { e ->
                        when (e) {
                            is QueueNotFoundException -> e
                            is IllegalArgumentException -> QueueNotFoundException(e.message ?: "큐를 찾을 수 없습니다")
                            else -> OperationFailedException(e.message ?: "큐 속성 수정 실패")
                        }
                    }
            }
    }

    @GetMapping("/{queueName}/stats")
    fun getQueueStats(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<QueueStatistics>> {
        logger.info("큐 통계 조회 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                queueService.getQueueStatistics(tenantId, queueName)
                    .map { stats -> ApiResponse.success(stats) }
                    .onErrorMap { e ->
                        when (e) {
                            is QueueNotFoundException -> e
                            is IllegalArgumentException -> QueueNotFoundException(e.message ?: "큐를 찾을 수 없습니다")
                            else -> OperationFailedException(e.message ?: "큐 통계 조회 실패")
                        }
                    }
            }
    }
}
