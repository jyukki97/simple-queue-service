package com.example.sqs.storage.service

import com.example.sqs.common.util.MessageParsingUtils
import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.MessageLocation
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierStats
import com.example.sqs.storage.dto.PromoteResult
import com.example.sqs.storage.dto.TierMessageInfo
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Service
class StorageStatsService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val partitioningService: QueuePartitioningService
) {

    private val logger = LoggerFactory.getLogger(StorageStatsService::class.java)

    /**
     * Hot Storage 통계 정보 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getHotStats(queue: Queue): Mono<TierStats> {
        return getPartitionedTierStats(queue, "HOT")
    }

    /**
     * Warm Storage 통계 정보 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getWarmStats(queue: Queue): Mono<TierStats> {
        return getPartitionedTierStats(queue, "WARM")
    }

    /**
     * Cold Storage 통계 정보 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getColdStats(queue: Queue): Mono<TierStats> {
        return getPartitionedTierStats(queue, "COLD")
    }

    /**
     * 메시지를 한 계층에서 다른 계층으로 이관 (모든 큐 타입에서 파티션 사용)
     */
    fun moveMessage(messageId: String, fromTier: String, toTier: String, queue: Queue): Mono<Void> {
        return moveMessageInPartitionedQueue(messageId, fromTier, toTier, queue)
    }

    /**
     * 파티션된 큐에서 메시지 이관 (FIFO와 Standard 모두 지원)
     */
    private fun moveMessageInPartitionedQueue(messageId: String, fromTier: String, toTier: String, queue: Queue): Mono<Void> {
        val partitionCount = 4 // 설정에서 가져와야 함
        val tierPrefix = fromTier.lowercase()

        // 모든 파티션에서 메시지를 찾아서 이관
        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
                val fromKey = "${tierPrefix}:queue:${queue.tenantId}:${partitionedQueueName}"

                // 해당 파티션에 메시지가 있는지 확인
                redisTemplate.opsForList()
                    .range(fromKey, 0, -1)
                    .any { it == messageId }
                    .flatMap { hasMessage ->
                        if (hasMessage) {
                            val toTierPrefix = toTier.lowercase()
                            val toKey = "${toTierPrefix}:queue:${queue.tenantId}:${partitionedQueueName}"
                            moveMessageBetweenKeys(messageId, fromKey, toKey, toTier)
                        } else {
                            Mono.empty()
                        }
                    }
            }
            .next()
            .then()
    }

    /**
     * 키 간 메시지 이관
     */
    private fun moveMessageBetweenKeys(messageId: String, fromKey: String, toKey: String, toTier: String): Mono<Void> {
        return redisTemplate.opsForList()
            .remove(fromKey, 1, messageId)
            .flatMap {
                when (toTier) {
                    "HOT" -> redisTemplate.opsForList().leftPush(toKey, messageId)
                    else -> redisTemplate.opsForList().rightPush(toKey, messageId)
                }
            }
            .then(updateMessageTier(messageId, toTier))
            .doOnSuccess {
                logger.debug("메시지 이관 완료: messageId=$messageId, $fromKey → $toKey")
            }
    }

    /**
     * 메시지 개수 조회
     */
    private fun getMessageCount(queueKey: String): Mono<Long> {
        return redisTemplate.opsForList()
            .size(queueKey)
            .defaultIfEmpty(0L)
    }

    /**
     * 가장 오래된 메시지의 타임스탬프 조회
     */
    private fun getOldestMessageTime(queueKey: String): Mono<Instant?> {
        return redisTemplate.opsForList()
            .index(queueKey, 0)  // 첫 번째 메시지 (가장 오래된)
            .flatMap { messageId ->
                redisTemplate.opsForValue()
                    .get("message:$messageId")
                    .map { messageJson ->
                        parseTimestampFromMessage(messageJson, messageId)
                    }
            }
            .switchIfEmpty(Mono.just(Instant.now())) // 메시지가 없으면 현재 시간
    }

    /**
     * 스토리지 크기 조회 (근사치)
     */
    private fun getStorageSize(queueKey: String): Mono<Long> {
        return redisTemplate.opsForList()
            .range(queueKey, 0, -1)
            .map { messageId -> messageId.length * 2L }  // UTF-8 근사치
            .reduce(0L) { acc, size -> acc + size }
            .defaultIfEmpty(0L)
    }

    /**
     * 파티션된 큐의 티어 통계 조회 (FIFO와 Standard 모두 지원)
     */
    private fun getPartitionedTierStats(queue: Queue, tier: String): Mono<TierStats> {
        val partitionCount = 4 // 기본 파티션 수, 실제로는 설정에서 가져와야 함
        val tierPrefix = tier.lowercase()

        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
                val tierKey = "${tierPrefix}:queue:${queue.tenantId}:${partitionedQueueName}"
                getSingleTierStats(tierKey, tier)
            }
            .reduce { stats1, stats2 ->
                TierStats(
                    tier = tier,
                    messageCount = stats1.messageCount + stats2.messageCount,
                    oldestMessageTime = if (stats1.oldestMessageTime != null && stats2.oldestMessageTime != null) {
                        if (stats1.oldestMessageTime.isBefore(stats2.oldestMessageTime)) stats1.oldestMessageTime else stats2.oldestMessageTime
                    } else stats1.oldestMessageTime ?: stats2.oldestMessageTime,
                    newestMessageTime = null,
                    totalSizeBytes = stats1.totalSizeBytes + stats2.totalSizeBytes
                )
            }
            .switchIfEmpty(
                Mono.just(
                    TierStats(
                        tier = tier,
                        messageCount = 0,
                        oldestMessageTime = null,
                        newestMessageTime = null,
                        totalSizeBytes = 0
                    )
                )
            )
    }

    /**
     * 메시지의 현재 위치 정보 조회
     */
    fun getMessageLocation(messageId: String): Mono<MessageLocation> {
        return redisTemplate.opsForHash<String, String>()
            .entries("message:$messageId")
            .collectMap({ it.key }, { it.value })
            .map { messageData ->
                MessageLocation(
                    tier = StorageTier.valueOf(messageData["tier"] ?: "HOT"),
                    lastMovedAt = messageData["movedAt"]?.let { Instant.parse(it) } ?: Instant.now(),
                    createdAt = messageData["createdAt"]?.let { Instant.parse(it) } ?: Instant.now()
                )
            }
    }

    /**
     * 메시지를 지정된 계층으로 승격
     */
    fun promoteMessage(messageId: String, targetTier: String, tenantId: String): Mono<PromoteResult> {
        return redisTemplate.opsForHash<String, String>()
            .get("message:$messageId", "tier")
            .flatMap { currentTier ->
                if (currentTier == targetTier) {
                    Mono.just(PromoteResult(currentTier, targetTier, false))
                } else {
                    performMessagePromotion(messageId, currentTier, targetTier, tenantId)
                        .map { PromoteResult(currentTier, targetTier, it) }
                }
            }
            .defaultIfEmpty(PromoteResult("UNKNOWN", targetTier, false))
    }

    /**
     * 실제 메시지 승격 수행
     */
    private fun performMessagePromotion(messageId: String, fromTier: String, toTier: String, tenantId: String): Mono<Boolean> {
        // 실제 구현에서는 메시지가 속한 큐를 찾아서 이관해야 함
        // 여기서는 간단하게 메시지 메타데이터만 업데이트
        return redisTemplate.opsForHash<String, String>()
            .putAll(
                "message:$messageId", mapOf(
                    "tier" to toTier,
                    "movedAt" to Instant.now().toString(),
                    "promotedManually" to "true"
                )
            )
            .map { true }
            .onErrorReturn(false)
    }

    /**
     * 메시지의 현재 계층 정보 업데이트
     */
    private fun updateMessageTier(messageId: String, tier: String): Mono<Void> {
        return redisTemplate.opsForHash<String, String>()
            .putAll(
                "message:$messageId", mapOf(
                    "tier" to tier,
                    "movedAt" to Instant.now().toString()
                )
            )
            .then()
    }

    /**
     * 특정 스토리지 계층의 메시지 목록 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getMessagesByTier(queue: Queue, tier: String, maxMessages: Int, offset: Int): Mono<List<TierMessageInfo>> {
        val partitionCount = 4 // 설정에서 가져와야 함
        val tierPrefix = when (tier.uppercase()) {
            "HOT" -> "hot:queue"
            "WARM" -> "warm:queue"
            "COLD" -> "cold:queue"
            else -> return Mono.just(emptyList())
        }

        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
                val tierKey = "$tierPrefix:${queue.tenantId}:$partitionedQueueName"

                redisTemplate.opsForList()
                    .range(tierKey, 0, -1) // 모든 메시지를 가져와서 offset/limit는 나중에 적용
                    .flatMap { messageId ->
                        getMessageDetails(messageId, tier)
                    }
            }
            .collectList()
            .map { allMessages ->
                // 메시지를 시간순으로 정렬 후 offset/limit 적용
                allMessages
                    .sortedBy { it.sentTimestamp }
                    .drop(offset)
                    .take(maxMessages)
            }
    }

    /**
     * 메시지 상세 정보 조회
     */
    private fun getMessageDetails(messageId: String, tier: String): Mono<TierMessageInfo> {
        return redisTemplate.opsForValue()
            .get("message:$messageId")
            .map { messageJson ->
                MessageParsingUtils.parseMessageData(messageJson)?.let { messageData ->
                    val body = messageData["messageBody"]?.toString() ?: ""
                    val attributesMap = messageData["attributes"] as? Map<String, Any> ?: emptyMap()
                    val attributes = attributesMap.mapValues { it.value.toString() }
                    val sentTimestamp = MessageParsingUtils.parseTimestampValue(messageData["sentTimestamp"]).toString()
                    val receiveCount = (messageData["receiveCount"] as? Number)?.toInt() ?: 0
                    val movedToTierAt = messageData["movedToTierAt"]?.toString()
                    val sizeBytes = body.length.toLong()

                    TierMessageInfo(
                        messageId = messageId,
                        messageBody = body,
                        attributes = attributes,
                        sentTimestamp = sentTimestamp,
                        receivedCount = receiveCount,
                        tier = tier,
                        movedToTierAt = movedToTierAt,
                        sizeBytes = sizeBytes,
                        compressionType = null
                    )
                } ?: createDefaultTierMessageInfo(messageId, tier, "파싱 실패")
            }
            .switchIfEmpty(
                Mono.just(createDefaultTierMessageInfo(messageId, tier, "메시지 없음"))
            )
    }

    /**
     * 기본 TierMessageInfo 생성
     */
    private fun createDefaultTierMessageInfo(messageId: String, tier: String, body: String): TierMessageInfo {
        return TierMessageInfo(
            messageId = messageId,
            messageBody = body,
            attributes = emptyMap(),
            sentTimestamp = Instant.now().toString(),
            receivedCount = 0,
            tier = tier,
            movedToTierAt = null,
            sizeBytes = 0L,
            compressionType = null
        )
    }

    /**
     * 단일 키의 티어 통계 조회
     */
    private fun getSingleTierStats(tierKey: String, tier: String): Mono<TierStats> {
        return Mono.zip(
            getMessageCount(tierKey),
            getOldestMessageTime(tierKey),
            getStorageSize(tierKey)
        ).map { tuple ->
            TierStats(
                tier = tier,
                messageCount = tuple.t1,
                oldestMessageTime = tuple.t2,
                newestMessageTime = null,
                totalSizeBytes = tuple.t3
            )
        }
    }

    /**
     * 메시지 JSON에서 타임스탬프 파싱
     */
    private fun parseTimestampFromMessage(messageJson: String, messageId: String): Instant {
        return try {
            val messageData = MessageParsingUtils.parseMessageData(messageJson)
            if (messageData != null) {
                MessageParsingUtils.parseTimestamp(messageData, "sentTimestamp")
            } else {
                logger.warn("메시지 데이터 파싱 실패: $messageId")
                Instant.now()
            }
        } catch (e: Exception) {
            logger.warn("메시지 파싱 실패: $messageId", e)
            Instant.now()
        }
    }

}
