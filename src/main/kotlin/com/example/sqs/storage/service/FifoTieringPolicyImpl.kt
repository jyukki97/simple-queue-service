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
    @Value("\${queue.partition-count:4}") private val partitionCount: Int,
    @Value("\${queue.hot.max-count:1000}") private val hotMaxCount: Int,
    @Value("\${queue.warm.max-count:5000}") private val warmMaxCount: Int,
    @Value("\${queue.hot.time-threshold-hours:2}") private val hotTimeThresholdHours: Long,
    @Value("\${queue.warm.time-threshold-hours:24}") private val warmTimeThresholdHours: Long
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

        return countExceeded || timeExceeded
    }

    override fun shouldMoveWarmToCold(stats: TierStats): Boolean {
        val countExceeded = stats.isCountExceeded(getWarmMaxCount())
        val timeExceeded = stats.isTimeExceeded(getWarmTimeThresholdHours())

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

        return hotFull && warmHasOlderMessages
    }

    override fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String> {
        logger.debug("FIFO Hot→Warm 이관 시작: ${queue.queueName}")

        return if (stats.isCountExceeded(getHotMaxCount())) {
            // 개수 초과: 모든 파티션에서 최신 메시지들을 이관 (FIFO 순서 유지)
            val totalMoveCount = stats.messageCount - getHotMaxCount()
            val moveCountPerPartition = totalMoveCount / partitionCount

            logger.info("FIFO Hot→Warm 개수 초과 이관: 총 ${totalMoveCount}개, 파티션당 ${moveCountPerPartition}개")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = getHotKeyForPartition(queue, partitionNumber)
                    getNewestMessages(hotKey, moveCountPerPartition)
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
            // 개수 초과: 모든 파티션에서 최신 메시지들을 이관
            val totalMoveCount = stats.messageCount - getWarmMaxCount()
            val moveCountPerPartition = totalMoveCount / partitionCount

            logger.info("FIFO Warm→Cold 개수 초과 이관: 총 ${totalMoveCount}개, 파티션당 ${moveCountPerPartition}개")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val warmKey = getWarmKeyForPartition(queue, partitionNumber)
                    getNewestMessages(warmKey, moveCountPerPartition)
                }
        } else {
            // 시간 초과: 모든 파티션에서 오래된 메시지들을 이관
            logger.info("FIFO Warm→Cold 시간 초과 이관")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val warmKey = getWarmKeyForPartition(queue, partitionNumber)
                    getExpiredMessages(warmKey, getWarmTimeThresholdHours())
                }
        }
    }

    override fun swapHotWarm(queue: Queue, hotStats: TierStats, warmStats: TierStats): Mono<SwapResult> {
        val startTime = System.currentTimeMillis()
        logger.debug("FIFO Hot-Warm 스왑 시작: ${queue.queueName}")

        // Hot의 최신 메시지들과 Warm의 오래된 메시지들을 교환
        val hotMoveCount = hotStats.messageCount - getHotMaxCount()
        val warmMoveCount = minOf(hotMoveCount, warmStats.messageCount)

        // 파티션별로 스왑 수행
        val hotMovePerPartition = hotMoveCount / partitionCount
        val warmMovePerPartition = warmMoveCount / partitionCount

        logger.info("FIFO 스왑: Hot→Warm ${hotMoveCount}개(파티션당${hotMovePerPartition}), Warm→Hot ${warmMoveCount}개(파티션당${warmMovePerPartition})")

        return Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val hotKey = getHotKeyForPartition(queue, partitionNumber)
                val warmKey = getWarmKeyForPartition(queue, partitionNumber)

                Mono.zip(
                    getNewestMessages(hotKey, hotMovePerPartition).collectList(),
                    getOldestMessages(warmKey, warmMovePerPartition).collectList()
                ).flatMap { tuple ->
                    val hotMessages = tuple.t1
                    val warmMessages = tuple.t2
                    performPartitionSwap(hotKey, warmKey, hotMessages, warmMessages)
                        .then(Mono.fromCallable { Pair(hotMessages.size, warmMessages.size) })
                }
            }
            .reduce(Pair(0, 0)) { acc, pair ->
                Pair(acc.first + pair.first, acc.second + pair.second)
            }
            .map { totalCounts ->
                SwapResult(
                    hotToWarmCount = totalCounts.first.toLong(),
                    warmToHotCount = totalCounts.second.toLong(),
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
     * 파티션별 Warm 키 생성
     */
    private fun getWarmKeyForPartition(queue: Queue, partitionNumber: Int): String {
        return "warm:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
    }

    /**
     * 파티션별 Hot-Warm 스왑 수행
     */
    private fun performPartitionSwap(hotKey: String, warmKey: String, hotMessages: List<String>, warmMessages: List<String>): Mono<Void> {
        val operations = mutableListOf<Mono<*>>()

        // Hot 메시지들을 Warm으로 이동
        hotMessages.forEach { messageId ->
            operations.add(
                redisTemplate.opsForList().remove(hotKey, 1, messageId)
                    .then(redisTemplate.opsForList().rightPush(warmKey, messageId))
                    .then(updateMessageTier(messageId, "WARM"))
            )
        }

        // Warm 메시지들을 Hot으로 이동
        warmMessages.forEach { messageId ->
            operations.add(
                redisTemplate.opsForList().remove(warmKey, 1, messageId)
                    .then(redisTemplate.opsForList().leftPush(hotKey, messageId))
                    .then(updateMessageTier(messageId, "HOT"))
            )
        }

        return if (operations.isNotEmpty()) {
            Mono.`when`(operations)
        } else {
            Mono.empty()
        }
    }

    /**
     * 가장 최신 메시지들 반환 (List의 끝에서부터)
     */
    private fun getNewestMessages(queueKey: String, count: Long): Flux<String> {
        return redisTemplate.opsForList()
            .range(queueKey, -count, -1)
    }

    /**
     * 가장 오래된 메시지들 반환 (List의 시작부터)
     */
    private fun getOldestMessages(queueKey: String, count: Long): Flux<String> {
        return redisTemplate.opsForList()
            .range(queueKey, 0, count - 1)
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
     * 실제 스왑 수행
     */
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
