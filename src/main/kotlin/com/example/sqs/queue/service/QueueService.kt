package com.example.sqs.queue.service

import com.example.sqs.common.exception.QueueNotFoundException
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.domain.QueueStatsResponse
import com.example.sqs.queue.repository.QueueRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Service
class QueueService(
    private val queueRepository: QueueRepository
) {
    
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
                queueRepository.delete(queue)
                // TODO: 큐의 모든 메시지도 삭제
            }
    }
    
    fun findQueue(tenantId: String, queueName: String): Mono<Queue> {
        return queueRepository.findByTenantIdAndQueueName(tenantId, queueName)
            .switchIfEmpty(Mono.error(QueueNotFoundException(queueName)))
    }
    
    fun listQueues(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue> {
        return queueRepository.findByTenantId(tenantId, limit, offset, namePattern)
    }
    
    fun getQueueStats(tenantId: String, queueName: String): Mono<QueueStatsResponse> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                queueRepository.getQueueStats(queue)
            }
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
    
    fun purgeQueue(tenantId: String, queueName: String): Mono<Void> {
        return findQueue(tenantId, queueName)
            .flatMap { queue ->
                queueRepository.purgeAllMessages(queue)
            }
    }
}
