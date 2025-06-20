package com.example.sqs.queue.service

import com.example.sqs.common.exception.QueueNotFoundException
import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.repository.QueueRepository
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Service
class QueueService(
    private val queueRepository: QueueRepository,
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val partitioningService: QueuePartitioningService,
    @Value("\${app.queue.partition-count:4}") private val partitionCount: Int
) {

    private val logger = LoggerFactory.getLogger(QueueService::class.java)

    fun createQueue(tenantId: String, queueName: String, attributes: QueueAttributes): Mono<Queue> {
        return queueRepository.existsByTenantIdAndQueueName(tenantId, queueName)
            .flatMap { exists ->
                if (exists) {
                    Mono.error(IllegalArgumentException("Queue already exists: $queueName"))
                } else {
                    val queue = Queue(
                        tenantId = tenantId,
                        queueName = queueName,
                        attributes = attributes,
                        createdAt = Instant.now(),
                        updatedAt = Instant.now()
                    )
                    queueRepository.save(queue)
                }
            }
    }

    fun deleteQueue(tenantId: String, queueName: String): Mono<Void> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                logger.info("큐 ${queueName} 삭제 시작")
                // 1. 먼저 큐 메타데이터 삭제
                queueRepository.delete(queue)
                    .doOnSuccess {
                        logger.info("큐 ${queueName} 메타데이터 삭제 완료")
                    }
                    .then(
                        // 2. 관련 Redis 키들 정리
                        cleanupQueueKeys(queue)
                    )
                    .doOnSuccess {
                        logger.info("큐 ${queueName} 완전 삭제 완료")
                    }
            }
    }

    /**
     * 큐 관련 Redis 키들 정리
     */
    private fun cleanupQueueKeys(queue: Queue): Mono<Void> {
        val keysToDelete = mutableListOf<String>()

        for (partitionNumber in 0 until partitionCount) {
            val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
            keysToDelete.addAll(
                listOf(
                    "hot:queue:${queue.tenantId}:$partitionedQueueName",
                    "warm:queue:${queue.tenantId}:$partitionedQueueName",
                    "cold:queue:${queue.tenantId}:$partitionedQueueName",
                    "hot:index:${queue.tenantId}:$partitionedQueueName",
                    "warm:index:${queue.tenantId}:$partitionedQueueName"
                )
            )
        }

        // 공통 키들
        keysToDelete.add("trace:index:${queue.tenantId}:${queue.queueName}")

        logger.info("큐 ${queue.queueName} 관련 ${keysToDelete.size}개 키 삭제 시작")

        return if (keysToDelete.isNotEmpty()) {
            redisTemplate.delete(*keysToDelete.toTypedArray())
                .doOnNext { deletedCount ->
                    logger.info("큐 ${queue.queueName} Redis 키 ${deletedCount}개 삭제 완료")
                }
                .then()
        } else {
            Mono.empty()
        }
    }

    fun findQueue(tenantId: String, queueName: String): Mono<Queue> {
        return queueRepository.findByTenantIdAndQueueName(tenantId, queueName)
            .switchIfEmpty(Mono.error(QueueNotFoundException(queueName)))
    }

    fun listQueues(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue> {
        return queueRepository.findByTenantId(tenantId, limit, offset, namePattern)
    }

    fun getQueueStatistics(tenantId: String, queueName: String): Mono<QueueStatistics> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                queueRepository.getQueueStatistics(queue)
            }
    }

    fun updateQueueAttributes(tenantId: String, queueName: String, attributes: QueueAttributes): Mono<Queue> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                val updatedQueue = queue.copy(
                    attributes = attributes,
                    updatedAt = Instant.now()
                )
                queueRepository.save(updatedQueue)
            }
    }
}
