package com.example.sqs.queue.repository

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatsResponse
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Repository
class RedisQueueRepository(
    private val redisTemplate: ReactiveRedisTemplate<String, Any>,
    private val objectMapper: ObjectMapper
) : QueueRepository {
    
    override fun save(queue: Queue): Mono<Queue> {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        val queueData = mapOf(
            "tenantId" to queue.tenantId,
            "queueName" to queue.queueName,
            "attributes" to objectMapper.writeValueAsString(queue.attributes),
            "createdAt" to queue.createdAt.toString(),
            "updatedAt" to queue.updatedAt.toString()
        )
        
        return redisTemplate.opsForHash<String, Any>().putAll(key, queueData)
            .then(
                // 테넌트별 큐 목록에도 추가
                redisTemplate.opsForSet().add("tenant:queues:${queue.tenantId}", queue.queueName)
                    .then(Mono.just(queue))
            )
    }
    
    override fun findByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Queue> {
        val key = "queue:${tenantId}:${queueName}"
        return redisTemplate.opsForHash<String, Any>().entries(key)
            .collectMap(
                { entry -> entry.key },
                { entry -> entry.value }
            )
            .filter { it.isNotEmpty() }
            .map { queueData ->
                val attributes = objectMapper.readValue(
                    queueData["attributes"] as String,
                    com.example.sqs.queue.domain.QueueAttributes::class.java
                )
                
                Queue(
                    tenantId = queueData["tenantId"] as String,
                    queueName = queueData["queueName"] as String,
                    attributes = attributes,
                    createdAt = Instant.parse(queueData["createdAt"] as String),
                    updatedAt = Instant.parse(queueData["updatedAt"] as String)
                )
            }
    }
    
    override fun existsByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Boolean> {
        val key = "queue:${tenantId}:${queueName}"
        return redisTemplate.hasKey(key)
    }
    
    override fun findByTenantId(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue> {
        val tenantQueuesKey = "tenant:queues:${tenantId}"
        return redisTemplate.opsForSet().members(tenantQueuesKey)
            .cast(String::class.java)
            .filter { queueName ->
                namePattern?.let { pattern ->
                    queueName.matches(pattern.replace("*", ".*").toRegex())
                } ?: true
            }
            .skip(offset.toLong())
            .take(limit.toLong())
            .flatMap { queueName ->
                findByTenantIdAndQueueName(tenantId, queueName)
            }
    }
    
    override fun delete(queue: Queue): Mono<Void> {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        val tenantQueuesKey = "tenant:queues:${queue.tenantId}"
        
        // 관련 메시지 큐들도 정리
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        return Mono.`when`(
            redisTemplate.delete(key),
            redisTemplate.opsForSet().remove(tenantQueuesKey, queue.queueName),
            redisTemplate.delete(messagesKey),
            redisTemplate.delete(delayedKey),
            redisTemplate.delete(inFlightKey)
        )
    }
    
    override fun getQueueStats(queue: Queue): Mono<QueueStatsResponse> {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        val availableMessagesMono = redisTemplate.opsForList().size(messagesKey).defaultIfEmpty(0L)
        val delayedMessagesMono = redisTemplate.opsForZSet().count(delayedKey, 
            org.springframework.data.domain.Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
            .defaultIfEmpty(0L)
        val inFlightMessagesMono = redisTemplate.opsForZSet().count(inFlightKey, 
            org.springframework.data.domain.Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
            .defaultIfEmpty(0L)
        
        return Mono.zip(availableMessagesMono, delayedMessagesMono, inFlightMessagesMono)
            .map { tuple ->
                QueueStatsResponse(
                    availableMessages = tuple.t1.toInt(),
                    inFlightMessages = tuple.t3.toInt(),
                    delayedMessages = tuple.t2.toInt(),
                    deadLetterMessages = 0 // TODO: DLQ 구현 후 추가
                )
            }
    }
    
    override fun purgeAllMessages(queue: Queue): Mono<Void> {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        return Mono.`when`(
            redisTemplate.delete(messagesKey),
            redisTemplate.delete(delayedKey),
            redisTemplate.delete(inFlightKey)
        )
    }
}
