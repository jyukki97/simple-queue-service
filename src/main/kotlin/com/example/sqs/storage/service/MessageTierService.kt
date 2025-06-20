package com.example.sqs.storage.service

import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.queue.domain.Queue
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux

/**
 * 메시지 계층 관리 서비스
 * 메시지가 어느 계층(HOT/WARM/COLD)에 있는지 찾고 관리하는 서비스
 */
@Service
class MessageTierService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val partitioningService: QueuePartitioningService
) {
    /**
     * 특정 계층의 메시지 목록 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getMessagesInTier(queue: Queue, tier: String, limit: Int = 10, offset: Int = 0): Flux<String> {
        val partitionCount = 4
        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
                val queueKey = "${tier.lowercase()}:queue:${queue.tenantId}:${partitionedQueueName}"
                redisTemplate.opsForList()
                    .range(queueKey, 0, -1)
            }
            .skip(offset.toLong())
            .take(limit.toLong())
    }
}
