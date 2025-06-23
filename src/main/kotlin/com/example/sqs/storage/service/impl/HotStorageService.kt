package com.example.sqs.storage.service.impl

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.StorageTierService
import com.example.sqs.storage.service.StorageUsageInfo
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactive.awaitFirstOrNull
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant

/**
 * 핫 스토리지 구현 (Redis Memory)
 * 즉시 처리가 필요한 메시지를 메모리에 저장
 */
@Service
class HotStorageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val redisKeyManager: RedisKeyManager
) : StorageTierService {

    override val tier = StorageTier.HOT

    companion object {
        private const val HOT_QUEUE_PREFIX = "hot:queue:"
        private const val DEFAULT_TTL_HOURS = 1L
    }

    override suspend fun saveMessage(message: Message): Boolean {
        return try {
            val messageKey = redisKeyManager.getMessageKey(message.messageId)
            val queueKey = redisKeyManager.getHotQueueKey(message.tenantId, message.queueName)
            val indexKey = redisKeyManager.getHotIndexKey(message.tenantId, message.queueName)

            val messageJson = objectMapper.writeValueAsString(message)
            val ttl = Duration.ofHours(DEFAULT_TTL_HOURS)

            // 메시지 저장 (TTL 설정)
            redisTemplate.opsForValue()
                .set(messageKey, messageJson, ttl)
                .awaitSingle()

            // 큐에 메시지 ID 추가 (FIFO 순서 보장)
            redisTemplate.opsForList()
                .rightPush(queueKey, message.messageId)
                .awaitSingle()

            // 큐 TTL 연장
            redisTemplate.expire(queueKey, ttl).awaitSingle()

            // 인덱스 업데이트 (빠른 조회용)
            redisTemplate.opsForZSet()
                .add(indexKey, message.messageId, message.sentTimestamp.epochSecond.toDouble())
                .awaitSingle()

            redisTemplate.expire(indexKey, ttl).awaitSingle()

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

        messages.forEach { message ->
            try {
                if (saveMessage(message)) {
                    successful++
                } else {
                    failed++
                    errors.add("Failed to save message: ${message.messageId}")
                }
            } catch (e: Exception) {
                failed++
                errors.add("Error saving message ${message.messageId}: ${e.message}")
            }
        }

        return BatchResult(successful, failed, errors)
    }

    override suspend fun getMessage(messageId: String): Message? {
        return try {
            val messageKey = redisKeyManager.getMessageKey(messageId)
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
            val queueKey = HOT_QUEUE_PREFIX + queueName

            // FIFO: 가장 왼쪽(오래된) 메시지 ID 조회
            val messageId = redisTemplate.opsForList()
                .leftPop(queueKey)
                .awaitFirstOrNull()

            messageId?.let { id ->
                val message = getMessage(id)

                // 메시지가 존재하지 않으면 다음 메시지 시도
                if (message == null) {
                    getOldestMessage(queueName)
                } else {
                    message
                }
            }
        } catch (e: Exception) {
            null
        }
    }

    override suspend fun getMessages(queueName: String, maxCount: Int): List<Message> {
        return try {
            val queueKey = HOT_QUEUE_PREFIX + queueName
            val messages = mutableListOf<Message>()

            repeat(maxCount) {
                val messageId = redisTemplate.opsForList()
                    .leftPop(queueKey)
                    .awaitFirstOrNull()

                if (messageId != null) {
                    getMessage(messageId)?.let { message ->
                        messages.add(message)
                    }
                } else {
                    return@repeat
                }
            }

            messages
        } catch (e: Exception) {
            emptyList()
        }
    }

    override suspend fun deleteMessage(messageId: String): Boolean {
        return try {
            val messageKey = redisKeyManager.getMessageKey(messageId)
            val deleted = redisTemplate.opsForValue()
                .delete(messageKey)
                .awaitSingle()

            updateMetrics(-1, 0)
            deleted
        } catch (e: Exception) {
            false
        }
    }

    override suspend fun deleteBatch(messageIds: List<String>): BatchResult {
        var successful = 0
        var failed = 0
        val errors = mutableListOf<String>()

        messageIds.forEach { messageId ->
            try {
                if (deleteMessage(messageId)) {
                    successful++
                } else {
                    failed++
                    errors.add("Failed to delete message: $messageId")
                }
            } catch (e: Exception) {
                failed++
                errors.add("Error deleting message $messageId: ${e.message}")
            }
        }

        return BatchResult(successful, failed, errors)
    }

    override suspend fun getExpiredMessages(maxAge: Duration, limit: Int): List<Message> {
        return try {
            val cutoffTime = Instant.now().minus(maxAge)
            val pattern = redisKeyManager.getMessagePattern()
            val expiredMessages = mutableListOf<Message>()

            // 패턴 매칭으로 모든 핫 메시지 키 조회
            redisTemplate.scan()
                .filter { it.startsWith(pattern.removeSuffix("*")) }
                .take(limit.toLong())
                .collectList()
                .awaitSingle()
                .forEach { key ->
                    try {
                        val messageJson = redisTemplate.opsForValue()
                            .get(key)
                            .awaitFirstOrNull()

                        messageJson?.let { json ->
                            val message = objectMapper.readValue(json, Message::class.java)
                            if (message.sentTimestamp.isBefore(cutoffTime)) {
                                expiredMessages.add(message)
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 메시지 오류는 무시하고 계속 진행
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
                .get(redisKeyManager.getHotMetricsKey())
                .awaitFirstOrNull()

            if (metricsJson != null) {
                objectMapper.readValue(metricsJson, TierMetrics::class.java)
            } else {
                // 기본 메트릭 생성
                createDefaultMetrics()
            }
        } catch (e: Exception) {
            createDefaultMetrics()
        }
    }

    override suspend fun getUsageInfo(): StorageUsageInfo {
        return try {
            // 간단한 메모리 사용량 추정
            val messageCount = getMessageCount()
            val estimatedUsage = messageCount * 1024 // 1KB per message 추정

            StorageUsageInfo(
                tier = StorageTier.HOT,
                totalCapacityBytes = 1000000000, // 1GB 기본값
                usedBytes = estimatedUsage,
                availableBytes = 1000000000 - estimatedUsage,
                usagePercent = if (estimatedUsage > 0) (estimatedUsage.toDouble() / 1000000000) * 100 else 0.0,
                messageCount = messageCount
            )
        } catch (e: Exception) {
            StorageUsageInfo(
                tier = StorageTier.HOT,
                totalCapacityBytes = 0,
                usedBytes = 0,
                availableBytes = 0,
                usagePercent = 0.0,
                messageCount = 0
            )
        }
    }

    private suspend fun getMessageCount(): Long {
        return try {
            redisTemplate.scan()
                .filter { it.startsWith(redisKeyManager.getMessagePrefix()) }
                .count()
                .awaitSingle()
        } catch (e: Exception) {
            0L
        }
    }

    private fun parseMemoryInfo(info: String, key: String): Long {
        return try {
            val line = info.lines().find { it.startsWith(key) }
            line?.substring(key.length)?.toLongOrNull() ?: 0L
        } catch (e: Exception) {
            0L
        }
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
                .set(redisKeyManager.getHotMetricsKey(), metricsJson, Duration.ofHours(24))
                .awaitSingleOrNull()
        } catch (e: Exception) {
            // 메트릭 업데이트 실패는 로깅만 하고 진행
        }
    }

    private fun createDefaultMetrics(): TierMetrics {
        return TierMetrics(
            tier = StorageTier.HOT,
            messageCount = 0,
            storageUsageBytes = 0,
            avgAccessTimeMs = 1.0, // 1ms 기본값
            hitRate = 0.0,
            migrationRate = 0.0,
            errorRate = 0.0,
            lastUpdated = Instant.now()
        )
    }
}
