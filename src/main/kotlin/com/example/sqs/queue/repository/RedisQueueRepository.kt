package com.example.sqs.queue.repository

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatsResponse
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.stereotype.Repository

@Repository
class RedisQueueRepository(
    private val redisTemplate: RedisTemplate<String, Any>,
    private val objectMapper: ObjectMapper
) : QueueRepository {
    
    override fun save(queue: Queue): Queue {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        val queueData = mapOf(
            "tenantId" to queue.tenantId,
            "queueName" to queue.queueName,
            "attributes" to objectMapper.writeValueAsString(queue.attributes),
            "createdAt" to queue.createdAt.toString(),
            "updatedAt" to queue.updatedAt.toString()
        )
        
        redisTemplate.opsForHash<String, Any>().putAll(key, queueData)
        
        // 테넌트별 큐 목록에도 추가
        val tenantQueuesKey = "tenant:queues:${queue.tenantId}"
        redisTemplate.opsForSet().add(tenantQueuesKey, queue.queueName)
        
        return queue
    }
    
    override fun findByTenantIdAndQueueName(tenantId: String, queueName: String): Queue? {
        val key = "queue:${tenantId}:${queueName}"
        val queueData = redisTemplate.opsForHash<String, Any>().entries(key)
        
        if (queueData.isEmpty()) {
            return null
        }
        
        return try {
            val attributes = objectMapper.readValue(
                queueData["attributes"] as String,
                com.example.sqs.queue.domain.QueueAttributes::class.java
            )
            
            Queue(
                tenantId = queueData["tenantId"] as String,
                queueName = queueData["queueName"] as String,
                attributes = attributes,
                createdAt = java.time.Instant.parse(queueData["createdAt"] as String),
                updatedAt = java.time.Instant.parse(queueData["updatedAt"] as String)
            )
        } catch (e: Exception) {
            null
        }
    }
    
    override fun existsByTenantIdAndQueueName(tenantId: String, queueName: String): Boolean {
        val key = "queue:${tenantId}:${queueName}"
        return redisTemplate.hasKey(key)
    }
    
    override fun findByTenantId(tenantId: String, limit: Int, offset: Int, namePattern: String?): List<Queue> {
        val tenantQueuesKey = "tenant:queues:${tenantId}"
        val queueNames = redisTemplate.opsForSet().members(tenantQueuesKey) ?: emptySet()
        
        val filteredNames = if (namePattern != null) {
            queueNames.filterIsInstance<String>().filter { it.matches(namePattern.replace("*", ".*").toRegex()) }
        } else {
            queueNames.filterIsInstance<String>()
        }
        
        return filteredNames
            .drop(offset)
            .take(limit)
            .mapNotNull { queueName ->
                findByTenantIdAndQueueName(tenantId, queueName)
            }
    }
    
    override fun delete(queue: Queue) {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        redisTemplate.delete(key)
        
        // 테넌트별 큐 목록에서도 제거
        val tenantQueuesKey = "tenant:queues:${queue.tenantId}"
        redisTemplate.opsForSet().remove(tenantQueuesKey, queue.queueName)
        
        // 관련 메시지 큐들도 정리
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        redisTemplate.delete(messagesKey)
        redisTemplate.delete(delayedKey)
        redisTemplate.delete(inFlightKey)
    }
    
    override fun getQueueStats(queue: Queue): QueueStatsResponse {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        val availableMessages = (redisTemplate.opsForList().size(messagesKey) ?: 0L).toInt()
        val delayedMessages = (redisTemplate.opsForZSet().count(delayedKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY) ?: 0L).toInt()
        val inFlightMessages = (redisTemplate.opsForZSet().count(inFlightKey, Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY) ?: 0L).toInt()
        
        return QueueStatsResponse(
            availableMessages = availableMessages,
            inFlightMessages = inFlightMessages,
            delayedMessages = delayedMessages,
            deadLetterMessages = 0 // TODO: DLQ 구현 후 추가
        )
    }
    
    override fun purgeAllMessages(queue: Queue) {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()
        
        redisTemplate.delete(messagesKey)
        redisTemplate.delete(delayedKey)
        redisTemplate.delete(inFlightKey)
    }
}
