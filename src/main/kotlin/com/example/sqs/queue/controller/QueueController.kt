package com.example.sqs.queue.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.domain.ErrorCode
import com.example.sqs.common.exception.QueueNotFoundException
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

    @DeleteMapping("/{queueName}")
    fun deleteQueue(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        logger.info("큐 삭제 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                queueService.deleteQueue(tenantId, queueName)
                    .then(Mono.just(ApiResponse.success("큐가 성공적으로 삭제되었습니다: $queueName")))
                    .onErrorResume { e ->
                        logger.error("큐 삭제 중 오류 발생", e)
                        when (e) {
                            is QueueNotFoundException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐 삭제 실패"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 삭제 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
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
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                queueService.listQueues(tenantId, limit, offset, namePattern)
                    .collectList()
                    .map { queues ->
                        logger.info("조회된 큐 개수: ${queues.size}")
                        ApiResponse.success(queues)
                    }
                    .onErrorResume { e ->
                        logger.error("큐 목록 조회 중 오류 발생", e)
                        Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 목록 조회 실패"))
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @GetMapping("/{queueName}/attributes")
    fun getQueueAttributes(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<Queue>> {
        logger.info("큐 속성 조회 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                queueService.findQueue(tenantId, queueName)
                    .map { queue ->
                        logger.info("큐 속성 조회 성공: ${queue.queueName}")
                        ApiResponse.success(queue)
                    }
                    .onErrorResume { e ->
                        logger.error("큐 속성 조회 중 오류 발생", e)
                        when (e) {
                            is QueueNotFoundException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 속성 조회 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }

    @PutMapping("/{queueName}/attributes")
    fun setQueueAttributes(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String,
        @RequestBody request: UpdateQueueRequest
    ): Mono<ApiResponse<Queue>> {
        logger.info("큐 속성 수정 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                queueService.updateQueueAttributes(tenantId, queueName, request.attributes)
                    .map { queue ->
                        logger.info("큐 속성 수정 성공: ${queue.queueName}")
                        ApiResponse.success(queue)
                    }
                    .onErrorResume { e ->
                        logger.error("큐 속성 수정 중 오류 발생", e)
                        when (e) {
                            is QueueNotFoundException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 속성 수정 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }
    
    @GetMapping("/{queueName}/statistics")
    fun getQueueStatistics(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<QueueStatistics>> {
        logger.info("큐 통계 조회 요청: $queueName")
        return tenantService.getTenantId(apiKey)
            .flatMap { tenantId ->
                logger.info("테넌트 확인: $tenantId")
                queueService.getQueueStatistics(tenantId, queueName)
                    .map { statistics ->
                        logger.info("큐 통계 조회 성공: ${statistics.queueName}")
                        ApiResponse.success(statistics)
                    }
                    .onErrorResume { e ->
                        logger.error("큐 통계 조회 중 오류 발생", e)
                        when (e) {
                            is QueueNotFoundException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            is IllegalArgumentException -> Mono.just(ApiResponse.error(ErrorCode.QUEUE_NOT_FOUND.name, e.message ?: "큐를 찾을 수 없습니다"))
                            else -> Mono.just(ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "큐 통계 조회 실패"))
                        }
                    }
            }
            .switchIfEmpty(Mono.just(ApiResponse.error(ErrorCode.UNAUTHORIZED.name, ErrorCode.UNAUTHORIZED.message)))
    }
}
