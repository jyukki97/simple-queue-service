package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.dto.BatchMoveResult
import com.example.sqs.common.redis.RedisKeyManager
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant

/**
 * WARM 계층 관리 서비스
 * Redis HOT → RocksDB WARM 계층 이동 처리
 */
@Service
class WarmTierManagerService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val rocksDBService: RocksDBWarmStorageService,
    private val objectMapper: ObjectMapper,
    private val redisKeyManager: RedisKeyManager
) {

    private val logger = LoggerFactory.getLogger(WarmTierManagerService::class.java)

    companion object {
        private const val WARM_TTL_HOURS = 24L
    }

    /**
     * HOT → WARM 계층으로 메시지 이동
     */
    fun moveMessageToWarm(messageId: String, tenantId: String, queueName: String): Mono<Boolean> {
        return getMessageFromRedis(messageId)
            .flatMap { message ->
                if (message != null) {
                    // RocksDB에 저장
                    rocksDBService.saveMessage(message, WARM_TTL_HOURS)
                        .flatMap { saved ->
                            if (saved) {
                                // Redis에서 제거
                                removeMessageFromRedis(messageId, tenantId, queueName)
                                    .then(updateMessageLocation(messageId, StorageTier.WARM))
                                    .map { true }
                            } else {
                                Mono.just(false)
                            }
                        }
                } else {
                    logger.warn("HOT에서 메시지를 찾을 수 없음: $messageId")
                    Mono.just(false)
                }
            }
            .onErrorResume { e ->
                logger.error("메시지 WARM 계층 이동 중 오류: $messageId", e)
                Mono.just(false)
            }
    }

    /**
     * 배치로 메시지를 WARM 계층으로 이동
     */
    fun moveMessagesToWarmBatch(messageIds: List<String>, tenantId: String, queueName: String): Mono<BatchMoveResult> {
        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                getMessageFromRedis(messageId)
                    .map { message -> messageId to message }
            }
            .collectList()
            .flatMap { messagesPairs ->
                val validMessages = messagesPairs.mapNotNull { (messageId, message) ->
                    if (message != null) message else {
                        logger.warn("메시지를 찾을 수 없음: $messageId")
                        null
                    }
                }

                if (validMessages.isNotEmpty()) {
                    // 배치로 RocksDB에 저장
                    rocksDBService.saveMessageBatch(validMessages, WARM_TTL_HOURS)
                        .flatMap { batchResult ->
                            // 성공한 메시지들만 Redis에서 제거
                            val successfulMessageIds = validMessages.map { it.messageId }
                            removeMessagesFromRedisBatch(successfulMessageIds, tenantId, queueName)
                                .then(updateMessageLocationsBatch(successfulMessageIds, StorageTier.WARM))
                                .then(
                                    Mono.just(
                                        BatchMoveResult(
                                            totalRequested = messageIds.size,
                                            successCount = batchResult.successCount,
                                            failCount = batchResult.failCount + (messageIds.size - validMessages.size),
                                            errors = batchResult.errors
                                        )
                                    )
                                )
                        }
                } else {
                    Mono.just(BatchMoveResult(messageIds.size, 0, messageIds.size, listOf("모든 메시지를 찾을 수 없음")))
                }
            }
    }

    /**
     * WARM 계층에서 메시지 조회
     */
    fun getMessageFromWarm(messageId: String): Mono<Message?> {
        return rocksDBService.getMessage(messageId)
    }

    /**
     * WARM 계층에서 메시지 삭제
     */
    fun deleteMessageFromWarm(messageId: String): Mono<Boolean> {
        return rocksDBService.deleteMessage(messageId)
            .flatMap { deleted ->
                if (deleted) {
                    // 위치 정보도 제거
                    removeMessageLocation(messageId)
                        .map { true }
                } else {
                    Mono.just(false)
                }
            }
    }

    /**
     * WARM 계층 만료된 메시지 정리
     */
    fun cleanupExpiredMessages(): Mono<Int> {
        return rocksDBService.cleanupExpiredMessages()
            .doOnSuccess { count ->
                logger.info("WARM 계층 만료 메시지 정리 완료: $count 개")
            }
    }

    /**
     * WARM 계층 통계 조회
     */
    fun getWarmStats(): Mono<WarmStorageStats> {
        return rocksDBService.getStorageStats()
    }

    /**
     * 큐별 WARM 메시지 조회 (파티션된 큐 이름도 포함)
     */
    fun getWarmMessagesByQueue(tenantId: String, queueName: String, limit: Int = 100, offset: Int = 0): Flux<Message> {
        // RocksDB에서 직접 큐 이름과 파티션 패턴 매칭으로 검색
        // getMessagesByQueue 메서드가 이미 파티션 패턴을 처리하므로 한 번만 호출
        return rocksDBService.getMessagesByQueue(tenantId, queueName, limit, offset)
    }

    /**
     * 디버그: RocksDB 키 개수 확인
     */
    fun debugKeyCount(): Mono<Map<String, Int>> {
        return rocksDBService.debugKeyCount()
    }

    /**
     * 디버그: 모든 메시지 정보 조회
     */
    fun debugAllMessages(): Mono<List<Map<String, Any>>> {
        return rocksDBService.debugAllMessages()
    }

    /**
     * 디버그: 처음 몇 개 메시지 정보 확인
     */
    fun debugFirstFewMessages(count: Int = 5): Mono<List<Map<String, String>>> {
        return rocksDBService.debugFirstFewMessages(count)
    }

    /**
     * Redis에서 메시지 조회
     */
    private fun getMessageFromRedis(messageId: String): Mono<Message?> {
        return redisTemplate.opsForValue()
            .get(redisKeyManager.getMessageKey(messageId))
            .map { messageJson -> objectMapper.readValue(messageJson, Message::class.java) }
    }

    /**
     * Redis에서 메시지 제거 (HOT 계층 큐에서)
     */
    private fun removeMessageFromRedis(messageId: String, tenantId: String, queueName: String): Mono<Void> {
        // 파티션 4개에서 메시지 제거 시도 (Standard 큐는 어느 파티션에 있는지 모르므로)
        val partitionCount = 4

        return Flux.merge(
            // 메시지 본체 삭제
            redisTemplate.delete(redisKeyManager.getMessageKey(messageId)),
            // 모든 파티션에서 제거 시도
            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    redisTemplate.opsForList().remove(
                        redisKeyManager.getHotQueueKey(tenantId, queueName, partitionNumber), 
                        1, messageId
                    )
                },
            // HOT 인덱스에서 제거
            redisTemplate.opsForZSet().remove(
                redisKeyManager.getHotIndexKey(tenantId, queueName), 
                messageId
            )
        ).then()
    }

    /**
     * 배치로 Redis에서 메시지 제거
     */
    private fun removeMessagesFromRedisBatch(messageIds: List<String>, tenantId: String, queueName: String): Mono<Void> {
        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                removeMessageFromRedis(messageId, tenantId, queueName)
            }
            .then()
    }

    /**
     * 메시지 위치 정보 업데이트 (해시 타입으로 저장)
     */
    private fun updateMessageLocation(messageId: String, tier: StorageTier): Mono<Boolean> {
        val locationKey = redisKeyManager.getMessageLocationKey(messageId)
        val updateData = mapOf(
            "tier" to tier.name,
            "lastMovedAt" to Instant.now().toString()
        )

        return redisTemplate.opsForHash<String, String>()
            .putAll(locationKey, updateData)
            .then(
                // TTL 설정
                redisTemplate.expire(locationKey, Duration.ofHours(48))
            )
            .map { true }
            .onErrorReturn(false)
    }

    /**
     * 메시지 위치 정보 제거 (해시 키 삭제)
     */
    private fun removeMessageLocation(messageId: String): Mono<Boolean> {
        val locationKey = redisKeyManager.getMessageLocationKey(messageId)
        return redisTemplate.delete(locationKey)
            .map { it > 0 }
            .onErrorReturn(false)
    }

    /**
     * 배치로 메시지 위치 정보 업데이트
     */
    private fun updateMessageLocationsBatch(messageIds: List<String>, tier: StorageTier): Mono<Void> {
        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                updateMessageLocation(messageId, tier)
            }
            .then()
    }

    /**
     * 큐의 모든 WARM 메시지 삭제
     */
    fun deleteAllQueueMessages(tenantId: String, queueName: String): Mono<Int> {
        logger.info("큐 ${tenantId}:${queueName} WARM 계층 메시지 삭제 시작")

        return rocksDBService.deleteQueueMessages(tenantId, queueName)
    }
}


