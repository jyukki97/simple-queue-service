package com.example.sqs.queue.controller

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.UpdateQueueRequest
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.service.QueueService
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import org.slf4j.LoggerFactory

@RestController
@RequestMapping("/api/v1/queues")
class QueueController(
    private val queueService: QueueService
) {
    private val logger = LoggerFactory.getLogger(QueueController::class.java)

    @PostMapping
    @ResponseStatus(HttpStatus.CREATED)
    fun createQueue(
        @RequestBody request: CreateQueueRequest
    ): Mono<ApiResponse<Queue>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("Received create queue request for: ${request.queueName}")
        return queueService.createQueue(tenantId, request.queueName, request.attributes)
            .map { queue -> ApiResponse.success(queue) }
    }

    @DeleteMapping("/{queueName}")
    fun deleteQueue(
        @PathVariable queueName: String
    ): Mono<ApiResponse<String>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("큐 삭제 요청: $queueName")
        return queueService.deleteQueue(tenantId, queueName)
            .then(Mono.just(ApiResponse.success("큐가 성공적으로 삭제되었습니다: $queueName")))
    }

    @GetMapping
    fun listQueues(
        @RequestParam(defaultValue = "20") limit: Int,
        @RequestParam(defaultValue = "0") offset: Int,
        @RequestParam(required = false) namePattern: String?
    ): Mono<ApiResponse<List<Queue>>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("큐 목록 조회 요청 - limit: $limit, offset: $offset, pattern: $namePattern")
        return queueService.listQueues(tenantId, limit, offset, namePattern)
            .collectList()
            .map { queues -> ApiResponse.success(queues) }
    }

    @GetMapping("/{queueName}/attributes")
    fun getQueueAttributes(
        @PathVariable queueName: String
    ): Mono<ApiResponse<Queue>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("큐 속성 조회 요청: $queueName")
        return queueService.findQueue(tenantId, queueName)
            .map { queue -> ApiResponse.success(queue) }
    }

    @PutMapping("/{queueName}/attributes")
    fun setQueueAttributes(
        @PathVariable queueName: String,
        @RequestBody request: UpdateQueueRequest
    ): Mono<ApiResponse<Queue>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("큐 속성 수정 요청: $queueName")
        return queueService.updateQueueAttributes(tenantId, queueName, request.attributes)
            .map { queue -> ApiResponse.success(queue) }
    }

    @GetMapping("/{queueName}/stats")
    fun getQueueStats(
        @PathVariable queueName: String
    ): Mono<ApiResponse<QueueStatistics>> {
        val tenantId = AuthContext.getCurrentTenantId()
        logger.info("큐 통계 조회 요청: $queueName")
        return queueService.getQueueStatistics(tenantId, queueName)
            .map { stats -> ApiResponse.success(stats) }
    }
}
