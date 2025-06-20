package com.example.sqs.storage.service

import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.StandardTieringPolicy
import com.example.sqs.storage.domain.TierStats
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import java.time.Instant

@Service
class StandardTieringPolicyImpl(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int
) : StandardTieringPolicy {

    private val logger = LoggerFactory.getLogger(StandardTieringPolicyImpl::class.java)

    override fun shouldMoveHotToWarm(stats: TierStats): Boolean {
        val countExceeded = stats.isCountExceeded(getHotMaxCount())
        val timeExceeded = stats.isTimeExceeded(getHotTimeThresholdHours())

        if (countExceeded) {
            logger.debug("Standard Hot 개수 초과: ${stats.messageCount} > ${getHotMaxCount()}")
        }
        if (timeExceeded) {
            logger.debug("Standard Hot 시간 초과: 가장 오래된 메시지 ${stats.oldestMessageTime}")
        }

        return countExceeded || timeExceeded
    }

    override fun shouldMoveWarmToCold(stats: TierStats): Boolean {
        val countExceeded = stats.isCountExceeded(getWarmMaxCount())
        val timeExceeded = stats.isTimeExceeded(getWarmTimeThresholdHours())

        if (countExceeded) {
            logger.debug("Standard Warm 개수 초과: ${stats.messageCount} > ${getWarmMaxCount()}")
        }
        if (timeExceeded) {
            logger.debug("Standard Warm 시간 초과: 가장 오래된 메시지 ${stats.oldestMessageTime}")
        }

        return countExceeded || timeExceeded
    }

    override fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String> {
        return if (stats.isCountExceeded(getHotMaxCount())) {
            // 개수 초과: 모든 파티션에서 오래된 메시지들을 이관
            val totalMoveCount = stats.messageCount - getHotMaxCount()
            val moveCountPerPartition = totalMoveCount / partitionCount

            logger.info("Standard Hot→Warm 이관: 총 ${totalMoveCount}개 메시지, 파티션당 ${moveCountPerPartition}개")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = "hot:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                    getOldestMessages(hotKey, moveCountPerPartition)
                }
        } else {
            // 시간 초과: 모든 파티션에서 만료된 메시지들을 이관
            logger.info("Standard Hot→Warm 시간 초과 이관")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = "hot:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                    getExpiredMessages(hotKey, getHotTimeThresholdHours())
                }
        }
    }

    override fun getMessagesToMoveWarmToCold(queue: Queue, stats: TierStats): Flux<String> {
        return if (stats.isCountExceeded(getWarmMaxCount())) {
            // 개수 초과: 모든 파티션에서 오래된 메시지들을 이관
            val totalMoveCount = stats.messageCount - getWarmMaxCount()
            val baseCountPerPartition = totalMoveCount / partitionCount
            val remainder = totalMoveCount % partitionCount

            logger.info("Standard Warm→Cold 이관: 총 ${totalMoveCount}개 메시지, 기본 파티션당 ${baseCountPerPartition}개, 나머지 ${remainder}개")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val warmKey = "warm:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"

                    // 나머지 분배: 처음 remainder개 파티션에서 1개씩 더 이동
                    val moveCountForThisPartition = if (partitionNumber < remainder) {
                        baseCountPerPartition + 1
                    } else {
                        baseCountPerPartition
                    }

                    if (moveCountForThisPartition > 0) {
                        getOldestMessages(warmKey, moveCountForThisPartition)
                    } else {
                        Flux.empty()
                    }
                }
        } else {
            // 시간 초과: 모든 파티션에서 만료된 메시지들을 이관
            logger.info("Standard Warm→Cold 시간 초과 이관")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val warmKey = "warm:queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                    getExpiredMessages(warmKey, getWarmTimeThresholdHours())
                }
        }
    }

    /**
     * 가장 오래된 메시지들 반환 (Standard 큐는 List의 시작부터)
     */
    private fun getOldestMessages(queueKey: String, count: Long): Flux<String> {
        if (count <= 0) {
            return Flux.empty()
        }

        return redisTemplate.opsForList()
            .size(queueKey)
            .flatMapMany { actualSize ->
                val safeCount = minOf(count, actualSize)
                if (safeCount > 0) {
                    redisTemplate.opsForList()
                        .range(queueKey, 0, safeCount - 1)
                } else {
                    Flux.empty()
                }
            }
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
     * 메시지 타임스탬프 조회
     */
    private fun getMessageTimestamp(messageId: String): reactor.core.publisher.Mono<Instant> {
        return redisTemplate.opsForHash<String, String>()
            .get("message:$messageId", "timestamp")
            .switchIfEmpty(
                redisTemplate.opsForHash<String, String>()
                    .get("message:$messageId", "sentTimestamp")
            )
            .switchIfEmpty(
                redisTemplate.opsForHash<String, String>()
                    .get("message:$messageId", "createdAt")
            )
            .map { timestampStr ->
                try {
                    Instant.parse(timestampStr)
                } catch (e: Exception) {
                    logger.warn("Failed to parse timestamp: $timestampStr", e)
                    Instant.now()
                }
            }
            .onErrorReturn(Instant.now())
    }
}
