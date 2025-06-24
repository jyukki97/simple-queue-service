package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.FifoTieringPolicy
import com.example.sqs.storage.domain.SwapResult
import com.example.sqs.storage.domain.TierStats
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Service
class FifoTieringPolicyImpl(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val warmTierManagerService: WarmTierManagerService,
    @Value("\${app.queue.partition-count:4}") private val partitionCount: Int,
    @Value("\${app.storage.tiering.fifo.hot-max-count:100}") private val hotMaxCount: Int,
    @Value("\${app.storage.tiering.fifo.warm-max-count:1000}") private val warmMaxCount: Int,
    @Value("\${app.storage.tiering.fifo.hot-time-threshold-hours:1}") private val hotTimeThresholdHours: Long,
    @Value("\${app.storage.tiering.fifo.warm-time-threshold-hours:24}") private val warmTimeThresholdHours: Long
) : FifoTieringPolicy {

    private val logger = LoggerFactory.getLogger(FifoTieringPolicyImpl::class.java)

    // 계층별 제한값 메서드들 override
    override fun getHotMaxCount(): Long = hotMaxCount.toLong()
    override fun getWarmMaxCount(): Long = warmMaxCount.toLong()
    override fun getHotTimeThresholdHours(): Long = hotTimeThresholdHours
    override fun getWarmTimeThresholdHours(): Long = warmTimeThresholdHours

    override fun shouldMoveHotToWarm(stats: TierStats): Boolean {
        val countExceeded = stats.isCountExceeded(getHotMaxCount())
        val timeExceeded = stats.isTimeExceeded(getHotTimeThresholdHours())

        logger.info("FIFO shouldMoveHotToWarm 검사: 메시지수=${stats.messageCount}, HOT최대=${getHotMaxCount()}, 개수초과=$countExceeded, 시간초과=$timeExceeded")

        return countExceeded || timeExceeded
    }

    override fun shouldMoveWarmToCold(stats: TierStats): Boolean {
        val countExceeded = stats.isCountExceeded(getWarmMaxCount())
        val timeExceeded = stats.isTimeExceeded(getWarmTimeThresholdHours())

        logger.info("FIFO shouldMoveWarmToCold 검사: 메시지수=${stats.messageCount}, WARM최대=${getWarmMaxCount()}, 개수초과=$countExceeded, 시간초과=$timeExceeded")

        return countExceeded || timeExceeded
    }

    override fun shouldSwapHotWarm(hotStats: TierStats, warmStats: TierStats): Boolean {
        // Hot이 가득 차고, Warm에 더 오래된 메시지가 있는 경우
        val hotFull = hotStats.isCountExceeded(getHotMaxCount())
        val warmHasOlderMessages = warmStats.oldestMessageTime?.let { warmOldest ->
            hotStats.oldestMessageTime?.let { hotOldest ->
                warmOldest.isBefore(hotOldest)
            } ?: false
        } ?: false

        logger.info("FIFO shouldSwapHotWarm 검사: HOT메시지수=${hotStats.messageCount}, WARM메시지수=${warmStats.messageCount}, HOT최대=${getHotMaxCount()}, HOT가득참=$hotFull, WARM에오래된메시지=$warmHasOlderMessages")

        return hotFull && warmHasOlderMessages
    }

    override fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String> {
        logger.debug("FIFO Hot→Warm 이관 시작: ${queue.queueName}")

        return if (stats.isCountExceeded(getHotMaxCount())) {
            // 개수 초과: 모든 파티션에서 최신 메시지들을 이관 (FIFO 순서 유지)
            val totalMoveCount = stats.messageCount - getHotMaxCount()
            val baseCountPerPartition = totalMoveCount / partitionCount
            val remainderCount = totalMoveCount % partitionCount

            logger.info("FIFO Hot→Warm 개수 초과 이관: 총 ${totalMoveCount}개, 기본 파티션당 ${baseCountPerPartition}개, 나머지 ${remainderCount}개")

            // 파티션별로 메시지 수를 계산 (나머지는 첫 번째 파티션들에 분배)
            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = getHotKeyForPartition(queue, partitionNumber)
                    val moveCount = if (partitionNumber < remainderCount) {
                        baseCountPerPartition + 1
                    } else {
                        baseCountPerPartition
                    }

                    if (moveCount > 0) {
                        logger.info("파티션 ${partitionNumber}에서 ${moveCount}개 메시지 이동")
                        getNewestMessages(hotKey, moveCount)
                    } else {
                        logger.info("파티션 ${partitionNumber}에서 이동할 메시지 없음")
                        Flux.empty()
                    }
                }
        } else {
            // 시간 초과: 모든 파티션에서 오래된 메시지들을 이관
            logger.info("FIFO Hot→Warm 시간 초과 이관")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = getHotKeyForPartition(queue, partitionNumber)
                    getExpiredMessages(hotKey, getHotTimeThresholdHours())
                }
        }
    }

    override fun getMessagesToMoveWarmToCold(queue: Queue, stats: TierStats): Flux<String> {
        logger.debug("FIFO Warm→Cold 이관 시작: ${queue.queueName}")

        return if (stats.isCountExceeded(getWarmMaxCount())) {
            // 개수 초과: RocksDB WARM에서 오래된 메시지들을 가져옴
            val totalMoveCount = stats.messageCount - getWarmMaxCount()

            logger.info("FIFO Warm→Cold 개수 초과 이관: 총 ${totalMoveCount}개")

            // RocksDB에서 오래된 메시지들을 가져옴 (FIFO 순서 유지)
            warmTierManagerService.getOldestMessages(queue.tenantId, queue.queueName, totalMoveCount.toInt())
                .doOnNext { messageId ->
                    logger.debug("WARM→COLD 이동 대상 메시지: $messageId")
                }
        } else {
            // 시간 초과: RocksDB WARM에서 만료된 메시지들을 가져옴
            logger.info("FIFO Warm→Cold 시간 초과 이관")

            warmTierManagerService.getExpiredMessages(queue.tenantId, queue.queueName, getWarmTimeThresholdHours())
                .doOnNext { messageId ->
                    logger.debug("WARM→COLD 시간 만료 메시지: $messageId")
                }
        }
    }

    override fun swapHotWarm(queue: Queue, hotStats: TierStats, warmStats: TierStats): Mono<SwapResult> {
        val startTime = System.currentTimeMillis()
        logger.debug("FIFO Hot-Warm 스왑 시작: ${queue.queueName}")

        // Hot(Redis)의 최신 메시지들과 Warm(RocksDB)의 오래된 메시지들을 교환
        val hotMoveCount = hotStats.messageCount - getHotMaxCount()
        val warmMoveCount = minOf(hotMoveCount, warmStats.messageCount)

        logger.info("FIFO 스왑: Hot(Redis)→Warm(RocksDB) ${hotMoveCount}개, Warm(RocksDB)→Hot(Redis) ${warmMoveCount}개")

        return Mono.zip(
            // Hot(Redis)에서 최신 메시지들을 가져와서 Warm(RocksDB)으로 이동
            if (hotMoveCount > 0) {
                getHotMessagesToMove(queue, hotMoveCount.toInt())
                    .flatMap { messageIds ->
                        moveMessagesFromHotToWarm(queue, messageIds)
                    }
            } else {
                Mono.just(0)
            },
            // Warm(RocksDB)에서 오래된 메시지들을 가져와서 Hot(Redis)으로 이동
            if (warmMoveCount > 0) {
                warmTierManagerService.getOldestMessages(queue.tenantId, queue.queueName, warmMoveCount.toInt())
                    .collectList()
                    .flatMap { messageIds ->
                        moveMessagesFromWarmToHot(queue, messageIds)
                    }
            } else {
                Mono.just(0)
            }
        ).map { result ->
            SwapResult(
                hotToWarmCount = result.t1.toLong(),
                warmToHotCount = result.t2.toLong(),
                executionTimeMs = System.currentTimeMillis() - startTime
            )
        }
    }

    /**
     * 파티션별 Hot 키 생성
     */
    private fun getHotKeyForPartition(queue: Queue, partitionNumber: Int): String {
        return "hot:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
    }

    /**
     * Hot(Redis)에서 이동할 최신 메시지들 가져오기
     */
    private fun getHotMessagesToMove(queue: Queue, count: Int): Mono<List<String>> {
        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val hotKey = getHotKeyForPartition(queue, partitionNumber)
                val countPerPartition = count / partitionCount + if (partitionNumber < count % partitionCount) 1 else 0
                if (countPerPartition > 0) {
                    getNewestMessages(hotKey, countPerPartition.toLong())
                } else {
                    Flux.empty()
                }
            }
            .collectList()
    }

    /**
     * Hot(Redis) → Warm(RocksDB) 메시지 이동
     */
    private fun moveMessagesFromHotToWarm(queue: Queue, messageIds: List<String>): Mono<Int> {
        if (messageIds.isEmpty()) {
            return Mono.just(0)
        }

        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                // RocksDB WARM으로 저장 (기존 moveMessageToWarm 메서드 사용)
                warmTierManagerService.moveMessageToWarm(messageId, queue.tenantId, queue.queueName)
                    .map { success -> if (success) 1 else 0 }
                    .onErrorResume { error ->
                        logger.error("Hot→Warm 이동 실패: messageId=$messageId", error)
                        Mono.just(0)
                    }
            }
            .reduce(0) { acc: Int, result: Int -> acc + result }
    }

    /**
     * Warm(RocksDB) → Hot(Redis) 메시지 이동
     */
    private fun moveMessagesFromWarmToHot(queue: Queue, messageIds: List<String>): Mono<Int> {
        if (messageIds.isEmpty()) {
            return Mono.just(0)
        }

        return Flux.fromIterable(messageIds)
            .flatMap { messageId ->
                // 1. RocksDB에서 메시지 데이터 가져오기
                warmTierManagerService.getMessageFromWarm(messageId)
                    .cast(Message::class.java)
                    .flatMap { message ->
                        // 2. Redis HOT으로 저장
                        saveMessageToHot(queue, message)
                            .then(
                                // 3. RocksDB에서 제거
                                warmTierManagerService.deleteMessageFromWarm(messageId)
                            )
                            .then(
                                // 4. 메시지 위치 정보 업데이트
                                updateMessageTier(messageId, "HOT")
                            )
                            .then(Mono.just(1))
                    }
                    .onErrorResume { error ->
                        logger.error("Warm→Hot 이동 실패: messageId=$messageId", error)
                        Mono.just(0)
                    }
            }
            .reduce(0) { acc: Int, result: Int -> acc + result }
    }

    /**
     * Hot(Redis)에 메시지 저장
     */
    private fun saveMessageToHot(queue: Queue, message: Message): Mono<Void> {
        val partitionNumber = message.messageId.hashCode() % partitionCount
        val hotKey = getHotKeyForPartition(queue, partitionNumber)

        return redisTemplate.opsForValue()
            .set("message:${message.messageId}", objectMapper.writeValueAsString(message))
            .cast(Boolean::class.java)
            .then(redisTemplate.opsForList().leftPush(hotKey, message.messageId))
            .cast(Long::class.java)
            .then()
    }

    /**
     * 가장 최신 메시지들 반환 (List의 끝에서부터)
     */
    private fun getNewestMessages(queueKey: String, count: Long): Flux<String> {
        return redisTemplate.opsForList()
            .range(queueKey, -count, -1)
    }

    /**
     * 시간이 만료된 메시지들 반환
     */
    private fun getExpiredMessages(queueKey: String, thresholdHours: Long): Flux<String> {
        val cutoffTime = Instant.now().minusSeconds(thresholdHours * 3600)

        return redisTemplate.opsForList()
            .range(queueKey, 0, -1)
            .filterWhen { messageId ->
                // 메시지 타임스탬프 확인
                getMessageTimestamp(messageId)
                    .map { timestamp -> timestamp.isBefore(cutoffTime) }
                    .defaultIfEmpty(false)
            }
    }

    /**
     * 메시지 타이어 정보 업데이트
     */
    private fun updateMessageTier(messageId: String, tier: String): Mono<Void> {
        return redisTemplate.opsForHash<String, String>()
            .put("message:location:$messageId", "tier", tier)
            .then(
                redisTemplate.opsForHash<String, String>()
                    .put("message:location:$messageId", "lastMovedAt", Instant.now().toString())
            )
            .then()
    }

    /**
     * 메시지 타임스탬프 조회
     */
    private fun getMessageTimestamp(messageId: String): Mono<Instant> {
        return redisTemplate.opsForValue()
            .get("message:$messageId")
            .map { messageJson ->
                val message = objectMapper.readValue(messageJson, Message::class.java)
                message.sentTimestamp
            }
    }
}
