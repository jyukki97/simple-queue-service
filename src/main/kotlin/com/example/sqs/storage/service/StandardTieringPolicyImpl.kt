package com.example.sqs.storage.service

import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.StandardTieringPolicy
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
class StandardTieringPolicyImpl(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val warmTierManagerService: WarmTierManagerService,
    private val objectMapper: ObjectMapper,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int
) : StandardTieringPolicy {

    private val logger = LoggerFactory.getLogger(StandardTieringPolicyImpl::class.java)

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

    override fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String> {
        return if (stats.isCountExceeded(getHotMaxCount())) {
            // 개수 초과: 모든 파티션에서 오래된 메시지들을 이관
            val totalMoveCount = stats.messageCount - getHotMaxCount()
            val moveCountPerPartition = totalMoveCount / partitionCount

            logger.info("Standard Hot→Warm 이관: 총 ${totalMoveCount}개 메시지, 파티션당 ${moveCountPerPartition}개")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = "queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                    getOldestMessages(hotKey, moveCountPerPartition)
                }
        } else {
            // 시간 초과: 모든 파티션에서 만료된 메시지들을 이관
            logger.info("Standard Hot→Warm 시간 초과 이관")

            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val hotKey = "queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                    getExpiredMessages(hotKey, getHotTimeThresholdHours())
                }
        }
    }

    override fun getMessagesToMoveWarmToCold(queue: Queue, stats: TierStats): Flux<String> {
        return if (stats.isCountExceeded(getWarmMaxCount())) {
            // 개수 초과: RocksDB에서 오래된 메시지들을 이관
            val totalMoveCount = stats.messageCount - getWarmMaxCount()

            logger.info("Standard Warm→Cold 이관: 총 ${totalMoveCount}개 메시지 이동 필요")

            // RocksDB에서 큐별 메시지를 조회하여 오래된 순으로 정렬 후 이동할 메시지 선택
            warmTierManagerService.getWarmMessagesByQueue(queue.tenantId, queue.queueName, Int.MAX_VALUE, 0)
                .sort { m1, m2 -> m1.sentTimestamp.compareTo(m2.sentTimestamp) } // 오래된 순으로 정렬
                .take(totalMoveCount)
                .map { it.messageId }
        } else {
            // 시간 초과: RocksDB에서 만료된 메시지들을 이관
            logger.info("Standard Warm→Cold 시간 초과 이관")

            val thresholdTime = Instant.now().minusSeconds(getWarmTimeThresholdHours() * 3600)

            warmTierManagerService.getWarmMessagesByQueue(queue.tenantId, queue.queueName, Int.MAX_VALUE, 0)
                .filter { message -> message.sentTimestamp.isBefore(thresholdTime) }
                .map { it.messageId }
        }
    }

    /**
     * 가장 오래된 메시지들 반환 (Standard 큐는 List의 시작부터)
     */
    private fun getOldestMessages(queueKey: String, count: Long): Flux<String> {
        if (count <= 0) {
            logger.debug("이동할 메시지 수가 0 이하: $count")
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
                    .defaultIfEmpty(false) // 타임스탬프를 확인할 수 없으면 만료되지 않은 것으로 처리
            }
    }

    /**
     * 메시지 타임스탬프 조회
     */
    private fun getMessageTimestamp(messageId: String): Mono<Instant> {
        return redisTemplate.opsForValue()
            .get("message:$messageId")
            .map { messageJson ->
                try {
                    val messageData = objectMapper.readTree(messageJson)
                    val timestampStr = messageData.get("sentTimestamp")?.asText()
                        ?: messageData.get("timestamp")?.asText()
                        ?: messageData.get("createdAt")?.asText()

                    if (timestampStr != null) {
                        Instant.parse(timestampStr)
                    } else {
                        logger.warn("메시지 $messageId 의 타임스탬프를 찾을 수 없음")
                        Instant.now()
                    }
                } catch (e: Exception) {
                    logger.warn("메시지 $messageId 타임스탬프 파싱 실패: ${e.message}")
                    Instant.now()
                }
            }
            .switchIfEmpty(
                // 메시지가 없으면 현재 시간 - 새 메시지가 바로 만료되지 않도록 함
                Mono.fromCallable {
                    logger.debug("메시지 $messageId 를 찾을 수 없음, 현재 시간으로 설정")
                    Instant.now()
                }
            )
            .onErrorReturn(Instant.now())
    }
}
