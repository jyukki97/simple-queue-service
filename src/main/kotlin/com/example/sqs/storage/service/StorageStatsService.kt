package com.example.sqs.storage.service

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.common.util.MessageParsingUtils
import com.example.sqs.message.domain.Message
import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.MessageLocation
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierStats
import com.example.sqs.storage.dto.PromoteResult
import com.example.sqs.storage.dto.TierMessageInfo
import com.example.sqs.storage.service.impl.ColdStorageService
import kotlinx.coroutines.reactor.mono
import org.slf4j.LoggerFactory
import org.springframework.context.annotation.Lazy
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Service
class StorageStatsService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val partitioningService: QueuePartitioningService,
    private val warmTierManagerService: WarmTierManagerService,
    private val redisKeyManager: RedisKeyManager,
    @Lazy private val coldStorageService: ColdStorageService
) {

    private val logger = LoggerFactory.getLogger(StorageStatsService::class.java)

    /**
     * Hot Storage 통계 정보 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getHotStats(queue: Queue): Mono<TierStats> {
        return getPartitionedTierStats(queue, "HOT")
    }

    /**
     * Warm Storage 통계 정보 조회 (RocksDB 사용)
     */
    fun getWarmStats(queue: Queue): Mono<TierStats> {
        // 큐별 WARM 메시지 개수 조회
        return warmTierManagerService.getWarmMessagesByQueue(queue.tenantId, queue.queueName, Int.MAX_VALUE, 0)
            .collectList()
            .map { messages ->
                val messageCount = messages.size.toLong()
                val totalSizeBytes = messages.sumOf { it.messageBody.length.toLong() }

                TierStats(
                    tier = "WARM",
                    messageCount = messageCount,
                    oldestMessageTime = messages.minByOrNull { it.sentTimestamp }?.sentTimestamp,
                    newestMessageTime = messages.maxByOrNull { it.sentTimestamp }?.sentTimestamp,
                    totalSizeBytes = totalSizeBytes
                )
            }
            .onErrorReturn(
                TierStats(
                    tier = "WARM",
                    messageCount = 0,
                    oldestMessageTime = null,
                    newestMessageTime = null,
                    totalSizeBytes = 0
                )
            )
    }

    /**
     * Cold Storage 통계 정보 조회 (파일 시스템 기반)
     */
    fun getColdStats(queue: Queue): Mono<TierStats> {
        return mono {
            // ColdStorageService에서 직접 메시지 조회
            val messages = coldStorageService.getMessages(queue.queueName, Int.MAX_VALUE)

            val messageCount = messages.size.toLong()
            val totalSizeBytes = messages.sumOf { it.messageBody.length.toLong() }
            val oldestMessageTime = messages.minByOrNull { it.sentTimestamp }?.sentTimestamp
            val newestMessageTime = messages.maxByOrNull { it.sentTimestamp }?.sentTimestamp

            TierStats(
                tier = "COLD",
                messageCount = messageCount,
                oldestMessageTime = oldestMessageTime,
                newestMessageTime = newestMessageTime,
                totalSizeBytes = totalSizeBytes
            )
        }
            .onErrorReturn(
                TierStats(
                    tier = "COLD",
                    messageCount = 0,
                    oldestMessageTime = null,
                    newestMessageTime = null,
                    totalSizeBytes = 0
                )
            )
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
                    .get(redisKeyManager.getMessageKey(messageId))
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
            .entries(redisKeyManager.getMessageLocationKey(messageId))
            .collectMap({ it.key }, { it.value })
            .map { locationData ->
                if (locationData.isEmpty()) {
                    // 위치 정보가 없으면 기본값 (HOT)
                    MessageLocation(
                        tier = StorageTier.HOT,
                        lastMovedAt = Instant.now(),
                        createdAt = Instant.now()
                    )
                } else {
                    MessageLocation(
                        tier = StorageTier.valueOf(locationData["tier"] ?: "HOT"),
                        lastMovedAt = locationData["lastMovedAt"]?.let { Instant.parse(it) } ?: Instant.now(),
                        createdAt = locationData["createdAt"]?.let { Instant.parse(it) } ?: Instant.now()
                    )
                }
            }
            .onErrorReturn(
                // Redis 에러 시 기본값 반환
                MessageLocation(
                    tier = StorageTier.HOT,
                    lastMovedAt = Instant.now(),
                    createdAt = Instant.now()
                )
            )
    }

    /**
     * 메시지를 지정된 계층으로 승격
     */
    fun promoteMessage(messageId: String, targetTier: String, tenantId: String): Mono<PromoteResult> {
        return redisTemplate.opsForHash<String, String>()
            .get(redisKeyManager.getMessageKey(messageId), "tier")
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
                redisKeyManager.getMessageKey(messageId), mapOf(
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
        return updateMessageLocation(messageId, StorageTier.valueOf(tier))
            .then()
    }

    /**
     * 특정 스토리지 계층의 메시지 목록 조회 (모든 큐 타입에서 파티션 사용)
     */
    fun getMessagesByTier(queue: Queue, tier: String, maxMessages: Int, offset: Int): Mono<List<TierMessageInfo>> {
        return when (tier.uppercase()) {
            "HOT" -> getHotTierMessages(queue, maxMessages, offset)
            "WARM" -> getWarmTierMessages(queue, maxMessages, offset)
            "COLD" -> getColdTierMessages(queue, maxMessages, offset)
            else -> Mono.just(emptyList())
        }
    }

    /**
     * HOT 계층 메시지 조회 (Redis)
     */
    private fun getHotTierMessages(queue: Queue, maxMessages: Int, offset: Int): Mono<List<TierMessageInfo>> {
        val partitionCount = 4 // 설정에서 가져와야 함

        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)
                val tierKey = redisKeyManager.getHotQueueKey(queue.tenantId, partitionedQueueName)

                redisTemplate.opsForList()
                    .range(tierKey, 0, -1)
                    .flatMap { messageId ->
                        getMessageDetails(messageId, "HOT")
                    }
            }
            .collectList()
            .map { allMessages ->
                allMessages
                    .sortedBy { it.sentTimestamp }
                    .drop(offset)
                    .take(maxMessages)
            }
    }

    /**
     * WARM 계층 메시지 조회 (RocksDB)
     */
    private fun getWarmTierMessages(queue: Queue, maxMessages: Int, offset: Int): Mono<List<TierMessageInfo>> {
        return warmTierManagerService.getWarmMessagesByQueue(queue.tenantId, queue.queueName, maxMessages, offset)
            .map { warmMessage ->
                TierMessageInfo(
                    messageId = warmMessage.messageId,
                    messageBody = warmMessage.messageBody,
                    attributes = warmMessage.attributes,
                    sentTimestamp = warmMessage.sentTimestamp.toString(),
                    receivedCount = warmMessage.receiveCount,
                    tier = "WARM",
                    movedToTierAt = null, // WARM 메시지에는 이 정보가 없을 수 있음
                    sizeBytes = warmMessage.messageBody.length.toLong(),
                    compressionType = null // WARM 메시지의 압축 타입 정보
                )
            }
            .collectList()
    }

    /**
     * COLD 계층 메시지 조회 (파일 시스템)
     */
    private fun getColdTierMessages(queue: Queue, maxMessages: Int, offset: Int): Mono<List<TierMessageInfo>> {
        return mono {
            // ColdStorageService에서 직접 메시지 조회
            val messages = coldStorageService.getMessages(queue.queueName, maxMessages + offset)

            messages.drop(offset).take(maxMessages).map { message ->
                TierMessageInfo(
                    messageId = message.messageId,
                    messageBody = message.messageBody,
                    attributes = message.attributes,
                    sentTimestamp = message.sentTimestamp.toString(),
                    receivedCount = message.receiveCount,
                    tier = "COLD",
                    movedToTierAt = null, // COLD 메시지의 이동 시간 정보는 별도 관리 필요
                    sizeBytes = message.messageBody.length.toLong(),
                    compressionType = "gzip" // COLD 계층은 gzip 압축 사용
                )
            }
        }
            .onErrorReturn(emptyList())
    }

    /**
     * 메시지 상세 정보 조회
     */
    private fun getMessageDetails(messageId: String, tier: String): Mono<TierMessageInfo> {
        return redisTemplate.opsForValue()
            .get(redisKeyManager.getMessageKey(messageId))
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

    /**
     * 메시지 위치 정보 저장
     */
    fun saveMessageLocation(messageId: String, tier: StorageTier, createdAt: Instant = Instant.now()): Mono<Boolean> {
        val locationData = mapOf(
            "tier" to tier.name,
            "lastMovedAt" to createdAt.toString(),
            "createdAt" to createdAt.toString()
        )

        return redisTemplate.opsForHash<String, String>()
            .putAll(redisKeyManager.getMessageLocationKey(messageId), locationData)
            .onErrorReturn(false)
    }

    /**
     * 메시지 위치 정보 업데이트 (계층 이동 시)
     */
    fun updateMessageLocation(messageId: String, newTier: StorageTier): Mono<Boolean> {
        val updateData = mapOf(
            "tier" to newTier.name,
            "lastMovedAt" to Instant.now().toString()
        )

        return redisTemplate.opsForHash<String, String>()
            .putAll(redisKeyManager.getMessageLocationKey(messageId), updateData)
            .onErrorReturn(false)
    }

    /**
     * WARM→COLD 배치 메시지 이동 (동시성 문제 해결)
     */
    fun moveMessagesBatchWarmToCold(messageIds: List<String>, queue: Queue): Mono<Long> {
        if (messageIds.isEmpty()) {
            return Mono.just(0L)
        }

        logger.info("WARM→COLD 배치 이동 시작: ${queue.queueName}, ${messageIds.size}개 메시지")

        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                warmTierManagerService.getMessageFromWarm(messageId)
                    .cast(Message::class.java)
                    .map { message -> messageId to message }
            }
            .collectList()
            .flatMap { messagesPairs ->
                val validMessages = messagesPairs.mapNotNull { (messageId, message) ->
                    if (message != null) message else {
                        logger.warn("WARM에서 메시지를 찾을 수 없음: $messageId")
                        null
                    }
                }

                if (validMessages.isNotEmpty()) {
                    // 배치로 COLD 저장소에 저장
                    mono {
                        coldStorageService.saveBatch(validMessages)
                    }
                        .flatMap { batchResult ->
                            if (batchResult.successCount > 0) {
                                // 성공한 메시지들을 WARM에서 제거
                                val successfulMessageIds = validMessages.take(batchResult.successCount).map { it.messageId }
                                Flux.fromIterable(successfulMessageIds)
                                    .flatMap { messageId ->
                                        warmTierManagerService.deleteMessageFromWarm(messageId)
                                            .then(updateMessageTier(messageId, "COLD"))
                                    }
                                    .count()
                                    .doOnSuccess { removedCount ->
                                        logger.info("WARM→COLD 배치 이동 완료: ${queue.queueName}, 성공: ${batchResult.successCount}개, WARM 제거: ${removedCount}개")
                                    }
                            } else {
                                logger.error("WARM→COLD 배치 저장 실패: ${queue.queueName}, 오류: ${batchResult.errors}")
                                Mono.just(0L)
                            }
                        }
                } else {
                    logger.warn("WARM→COLD 배치 이동: 유효한 메시지가 없음")
                    Mono.just(0L)
                }
            }
    }

}
