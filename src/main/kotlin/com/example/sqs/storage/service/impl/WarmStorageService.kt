package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.*
import com.example.sqs.storage.service.*
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant

/**
 * 웜 스토리지 구현 (Redis Disk/RDB)
 * 중간 빈도 접근 메시지와 메타데이터를 저장
 */
@Service
class WarmStorageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper
) : StorageTierService {
    
    override val tier = StorageTier.WARM
    
    companion object {
        private const val WARM_MESSAGE_PREFIX = "warm:message:"
        private const val WARM_QUEUE_PREFIX = "warm:queue:"
        private const val WARM_INDEX_PREFIX = "warm:index:"
        private const val WARM_METADATA_PREFIX = "warm:meta:"
        private const val WARM_METRICS_KEY = "warm:metrics"
        private const val DEFAULT_TTL_DAYS = 1L
    }
    
    override suspend fun saveMessage(message: Message): Boolean {
        return try {
            val messageKey = WARM_MESSAGE_PREFIX + message.messageId
            val queueKey = WARM_QUEUE_PREFIX + "${message.tenantId}:${message.queueName}"
            val indexKey = WARM_INDEX_PREFIX + "${message.tenantId}:${message.queueName}"
            val metadataKey = WARM_METADATA_PREFIX + message.messageId
            
            val messageJson = objectMapper.writeValueAsString(message)
            val ttl = Duration.ofDays(DEFAULT_TTL_DAYS)
            
            // 메시지 저장 (긴 TTL 설정)
            redisTemplate.opsForValue()
                .set(messageKey, messageJson, ttl)
                .awaitSingle()
            
            // 큐에 메시지 ID 추가 (정렬된 집합으로 순서 유지)
            redisTemplate.opsForZSet()
                .add(queueKey, message.messageId, message.sentTimestamp.epochSecond.toDouble())
                .awaitSingle()
            
            redisTemplate.expire(queueKey, ttl).awaitSingle()
            
            // 메타데이터 저장 (빠른 검색용)
            val metadata = createMessageMetadata(message)
            redisTemplate.opsForHash<String, String>()
                .putAll(metadataKey, metadata)
                .awaitSingle()
            
            redisTemplate.expire(metadataKey, ttl).awaitSingle()
            
            // 메트릭 업데이트
            updateMetrics(1, 0)
            
            true
        } catch (e: Exception) {
            updateMetrics(0, 1)
            false
        }
    }
    
    override suspend fun saveBatch(messages: List<Message>): BatchResult {
        var successful = 0
        var failed = 0
        val errors = mutableListOf<String>()
        
        // 큐별로 그룹화하여 배치 처리 최적화
        messages.groupBy { "${it.tenantId}:${it.queueName}" }.forEach { (queueKey, queueMessages) ->
            try {
                saveBatchForQueue(queueKey, queueMessages)
                successful += queueMessages.size
            } catch (e: Exception) {
                failed += queueMessages.size
                errors.add("Batch save failed for queue $queueKey: ${e.message}")
            }
        }
        
        return BatchResult(successful, failed, errors)
    }
    
    private suspend fun saveBatchForQueue(queueKey: String, messages: List<Message>) {
        val warmQueueKey = WARM_QUEUE_PREFIX + queueKey
        val ttl = Duration.ofDays(DEFAULT_TTL_DAYS)
        
        // 배치로 정렬된 집합에 추가
        val scoreMembers = messages.map { message ->
            org.springframework.data.redis.core.DefaultTypedTuple(
                message.messageId,
                message.sentTimestamp.epochSecond.toDouble()
            )
        }.toSet()
        
        redisTemplate.opsForZSet()
            .addAll(warmQueueKey, scoreMembers)
            .awaitSingle()
        
        redisTemplate.expire(warmQueueKey, ttl).awaitSingle()
        
        // 개별 메시지와 메타데이터 저장
        messages.forEach { message ->
            val messageKey = WARM_MESSAGE_PREFIX + message.messageId
            val metadataKey = WARM_METADATA_PREFIX + message.messageId
            
            val messageJson = objectMapper.writeValueAsString(message)
            redisTemplate.opsForValue()
                .set(messageKey, messageJson, ttl)
                .awaitSingle()
            
            val metadata = createMessageMetadata(message)
            redisTemplate.opsForHash<String, String>()
                .putAll(metadataKey, metadata)
                .awaitSingle()
            
            redisTemplate.expire(metadataKey, ttl).awaitSingle()
        }
    }
    
    override suspend fun getMessage(messageId: String): Message? {
        return try {
            val messageKey = WARM_MESSAGE_PREFIX + messageId
            val messageJson = redisTemplate.opsForValue()
                .get(messageKey)
                .awaitFirstOrNull()
            
            messageJson?.let { 
                objectMapper.readValue(it, Message::class.java)
            }
        } catch (e: Exception) {
            null
        }
    }
    
    override suspend fun getOldestMessage(queueName: String): Message? {
        return try {
            val queueKey = WARM_QUEUE_PREFIX + queueName
            
            // 정렬된 집합에서 가장 작은 점수(가장 오래된) 메시지 ID 조회
            val messageIds = redisTemplate.opsForZSet()
                .range(queueKey, org.springframework.data.domain.Range.closed(0L, 0L))
                .collectList()
                .awaitSingle()
            
            if (messageIds.isNotEmpty()) {
                val messageId = messageIds.first()
                
                // 메시지 조회 후 큐에서 제거
                val message = getMessage(messageId)
                if (message != null) {
                    redisTemplate.opsForZSet()
                        .remove(queueKey, messageId)
                        .awaitSingle()
                    message
                } else {
                    // 메시지가 없으면 큐에서도 제거하고 다음 시도
                    redisTemplate.opsForZSet()
                        .remove(queueKey, messageId)
                        .awaitSingle()
                    getOldestMessage(queueName)
                }
            } else {
                null
            }
        } catch (e: Exception) {
            null
        }
    }
    
    override suspend fun getMessages(queueName: String, maxCount: Int): List<Message> {
        return try {
            val queueKey = WARM_QUEUE_PREFIX + queueName
            val messages = mutableListOf<Message>()
            
            // 정렬된 집합에서 여러 메시지 ID 조회
            val messageIds = redisTemplate.opsForZSet()
                .range(queueKey, org.springframework.data.domain.Range.closed(0L, (maxCount - 1).toLong()))
                .collectList()
                .awaitSingle()
            
            messageIds.forEach { messageId ->
                getMessage(messageId)?.let { message ->
                    messages.add(message)
                    
                    // 큐에서 제거
                    redisTemplate.opsForZSet()
                        .remove(queueKey, messageId)
                        .awaitSingle()
                }
            }
            
            messages
        } catch (e: Exception) {
            emptyList()
        }
    }
    
    override suspend fun deleteMessage(messageId: String): Boolean {
        return try {
            val messageKey = WARM_MESSAGE_PREFIX + messageId
            val metadataKey = WARM_METADATA_PREFIX + messageId
            
            // 메시지와 메타데이터 모두 삭제
            val messageDeleted = redisTemplate.opsForValue()
                .delete(messageKey)
                .awaitSingle()
            
            redisTemplate.delete(metadataKey).awaitSingle()
            
            updateMetrics(-1, 0)
            messageDeleted
        } catch (e: Exception) {
            false
        }
    }
    
    override suspend fun deleteBatch(messageIds: List<String>): BatchResult {
        var successful = 0
        var failed = 0
        val errors = mutableListOf<String>()
        
        try {
            // 배치 삭제 최적화
            val messageKeys = messageIds.map { WARM_MESSAGE_PREFIX + it }
            val metadataKeys = messageIds.map { WARM_METADATA_PREFIX + it }
            
            val deletedCount = redisTemplate.delete(*(messageKeys + metadataKeys).toTypedArray())
                .awaitSingle()
            
            successful = minOf(messageIds.size, deletedCount.toInt() / 2) // 메시지와 메타데이터로 나누기
            failed = messageIds.size - successful
            
            updateMetrics(-successful, 0)
        } catch (e: Exception) {
            failed = messageIds.size
            errors.add("Batch delete failed: ${e.message}")
        }
        
        return BatchResult(successful, failed, errors)
    }
    
    override suspend fun getExpiredMessages(maxAge: Duration, limit: Int): List<Message> {
        return try {
            val cutoffTime = Instant.now().minus(maxAge)
            val expiredMessages = mutableListOf<Message>()
            
            // 메타데이터를 통해 효율적으로 만료된 메시지 찾기
            redisTemplate.scan()
                .filter { it.startsWith(WARM_METADATA_PREFIX) }
                .take(limit.toLong())
                .collectList()
                .awaitSingle()
                .forEach { metadataKey ->
                    try {
                        val sentTimeStr = redisTemplate.opsForHash<String, String>()
                            .get(metadataKey, "sentTimestamp")
                            .awaitFirstOrNull()
                        
                        sentTimeStr?.let { timeStr ->
                            val sentTime = Instant.parse(timeStr)
                            if (sentTime.isBefore(cutoffTime)) {
                                val messageId = metadataKey.removePrefix(WARM_METADATA_PREFIX)
                                getMessage(messageId)?.let { message ->
                                    expiredMessages.add(message)
                                }
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 메시지 오류는 무시
                    }
                }
            
            expiredMessages
        } catch (e: Exception) {
            emptyList()
        }
    }
    
    override suspend fun getMetrics(): TierMetrics {
        return try {
            val metricsJson = redisTemplate.opsForValue()
                .get(WARM_METRICS_KEY)
                .awaitFirstOrNull()
            
            if (metricsJson != null) {
                objectMapper.readValue(metricsJson, TierMetrics::class.java)
            } else {
                createDefaultMetrics()
            }
        } catch (e: Exception) {
            createDefaultMetrics()
        }
    }
    
    override suspend fun getUsageInfo(): StorageUsageInfo {
        return try {
            val messageCount = getMessageCount()
            val estimatedSizeBytes = messageCount * 1024 // 평균 1KB per message 추정
            
            StorageUsageInfo(
                tier = StorageTier.WARM,
                totalCapacityBytes = Long.MAX_VALUE, // 디스크 기반이므로 제한 없음으로 설정
                usedBytes = estimatedSizeBytes,
                availableBytes = Long.MAX_VALUE - estimatedSizeBytes,
                usagePercent = 0.0, // 디스크 사용량은 별도 모니터링 필요
                messageCount = messageCount
            )
        } catch (e: Exception) {
            StorageUsageInfo(
                tier = StorageTier.WARM,
                totalCapacityBytes = 0,
                usedBytes = 0,
                availableBytes = 0,
                usagePercent = 0.0,
                messageCount = 0
            )
        }
    }
    
    /**
     * 콜드 이관 후보 조회
     */
    suspend fun getColdMigrationCandidates(maxAge: Duration, maxBatchSize: Int): List<Message> {
        return getExpiredMessages(maxAge, maxBatchSize)
    }
    
    private suspend fun getMessageCount(): Long {
        return try {
            redisTemplate.scan()
                .filter { it.startsWith(WARM_MESSAGE_PREFIX) }
                .count()
                .awaitSingle()
        } catch (e: Exception) {
            0L
        }
    }
    
    private fun createMessageMetadata(message: Message): Map<String, String> {
        return mapOf(
            "messageId" to message.messageId,
            "tenantId" to message.tenantId,
            "queueName" to message.queueName,
            "sentTimestamp" to message.sentTimestamp.toString(),
            "receiveCount" to message.receiveCount.toString(),
            "sizeBytes" to message.messageBody.toByteArray().size.toString()
        )
    }
    
    private suspend fun updateMetrics(messageCountDelta: Int, errorCountDelta: Int) {
        try {
            val currentMetrics = getMetrics()
            val updatedMetrics = currentMetrics.copy(
                messageCount = maxOf(0, currentMetrics.messageCount + messageCountDelta),
                errorRate = currentMetrics.errorRate + errorCountDelta,
                lastUpdated = Instant.now()
            )
            
            val metricsJson = objectMapper.writeValueAsString(updatedMetrics)
            redisTemplate.opsForValue()
                .set(WARM_METRICS_KEY, metricsJson, Duration.ofDays(7))
                .awaitSingleOrNull()
        } catch (e: Exception) {
            // 메트릭 업데이트 실패는 로깅만 하고 진행
        }
    }
    
    private fun createDefaultMetrics(): TierMetrics {
        return TierMetrics(
            tier = StorageTier.WARM,
            messageCount = 0,
            storageUsageBytes = 0,
            avgAccessTimeMs = 5.0, // 5ms 기본값
            hitRate = 0.0,
            migrationRate = 0.0,
            errorRate = 0.0,
            lastUpdated = Instant.now()
        )
    }
}
