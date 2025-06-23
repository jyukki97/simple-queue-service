package com.example.sqs.queue.service

import com.example.sqs.common.exception.QueueNotFoundException
import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.QueueListResponse
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.repository.QueueRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

/**
 * 큐 관리 관련 핵심 비즈니스 로직을 담당하는 서비스
 * 리팩토링: BaseReactiveService 상속으로 공통 기능 활용
 */
@Service
class QueueService(
    private val queueRepository: QueueRepository,
    private val queueValidationService: QueueValidationService,
    private val queueCleanupService: QueueCleanupService
) : BaseReactiveService() {

    override fun getServiceName(): String = "QueueService"

    /**
     * 새로운 큐를 생성합니다
     */
    fun createQueue(tenantId: String, queueName: String, attributes: QueueAttributes): Mono<Queue> {
        return queueRepository.existsByTenantIdAndQueueName(tenantId, queueName)
            .flatMap { exists ->
                if (exists) {
                    Mono.error(IllegalArgumentException("Queue already exists: $queueName"))
                } else {
                    // 큐 검증 (위임)
                    queueValidationService.validateQueueCreationRequest(tenantId, queueName, attributes)

                    val queue = Queue(
                        tenantId = tenantId,
                        queueName = queueName,
                        attributes = attributes,
                        createdAt = Instant.now(),
                        updatedAt = Instant.now()
                    )
                    queueRepository.save(queue)
                        .doOnSuccess {
                            // 실무 필요: 큐 생성 성공 로그
                            logger.info("Queue created successfully - tenant: $tenantId, queue: $queueName, fifo: ${attributes.fifoQueue}")
                        }
                }
            }
            .onErrorResume(handleError("createQueue"))
    }

    /**
     * 큐를 삭제합니다 (정리 서비스로 위임)
     */
    fun deleteQueue(tenantId: String, queueName: String): Mono<Void> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                queueCleanupService.cleanupQueueData(queue)
                    .then(queueRepository.delete(queue))
            }
            .doOnSuccess {
                logger.info("[${getServiceName()}] Queue deleted successfully: tenantId=$tenantId, queueName=$queueName")
            }
            .onErrorResume(handleError("deleteQueue"))
    }

    /**
     * 큐를 조회합니다
     */
    fun findQueue(tenantId: String, queueName: String): Mono<Queue> {
        return queueRepository.findByTenantIdAndQueueName(tenantId, queueName)
            .switchIfEmpty(
                Mono.error(QueueNotFoundException("Queue not found: $queueName for tenant: $tenantId"))
            )
    }

    /**
     * 테넌트의 모든 큐를 조회합니다
     */
    fun findQueuesByTenant(tenantId: String, limit: Int = 1000, offset: Int = 0, namePrefix: String? = null): Flux<Queue> {
        return queueRepository.findByTenantId(tenantId, limit, offset, namePrefix)
            .onErrorResume(handleFluxError("findQueuesByTenant"))
    }

    /**
     * 큐 속성을 업데이트합니다
     */
    fun updateQueueAttributes(tenantId: String, queueName: String, attributes: QueueAttributes): Mono<Queue> {
        return findQueue(tenantId, queueName)
            .flatMap { existingQueue ->
                // 속성 검증 (위임)
                queueValidationService.validateQueueAttributesUpdate(existingQueue, attributes)

                val updatedQueue = existingQueue.copy(
                    attributes = attributes,
                    updatedAt = Instant.now()
                )
                queueRepository.save(updatedQueue)
            }
            .onErrorResume(handleError("updateQueueAttributes"))
    }

    /**
     * 큐 통계를 조회합니다
     */
    fun getQueueStatistics(tenantId: String, queueName: String): Mono<QueueStatistics> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                queueRepository.getQueueStatistics(queue)
            }
            .onErrorResume(handleError("getQueueStatistics"))
    }

    /**
     * 큐 존재 여부를 확인합니다
     */
    fun queueExists(tenantId: String, queueName: String): Mono<Boolean> {
        return queueRepository.existsByTenantIdAndQueueName(tenantId, queueName)
            .onErrorReturn(false)
    }

    /**
     * 큐 목록을 페이징하여 조회합니다
     */
    fun getQueuesWithPaging(tenantId: String, pageSize: Int, pageToken: String?): Mono<QueueListResponse> {
        val validPageSize = pageSize.coerceIn(1, 1000)
        val offset = pageToken?.toIntOrNull() ?: 0

        return findQueuesByTenant(tenantId, validPageSize + 1, offset)
            .collectList()
            .map { queues ->
                val hasMore = queues.size > validPageSize
                val resultQueues = if (hasMore) queues.dropLast(1) else queues
                val nextPageToken = if (hasMore) (offset + validPageSize).toString() else null

                QueueListResponse(
                    queues = resultQueues,
                    nextPageToken = nextPageToken
                )
            }
            .onErrorResume(handleError("getQueuesWithPaging"))
    }

    /**
     * 큐 목록을 조회합니다 (컨트롤러용)
     */
    fun listQueues(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue> {
        return findQueuesByTenant(tenantId, limit, offset, namePattern)
    }
}
