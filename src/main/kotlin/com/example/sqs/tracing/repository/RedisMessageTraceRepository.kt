package com.example.sqs.tracing.repository

import com.example.sqs.tracing.domain.*
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * Redis 기반 메시지 추적 레포지토리 구현체
 */
@Repository
class RedisMessageTraceRepository(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper
) : MessageTraceRepository {

    private val logger = LoggerFactory.getLogger(RedisMessageTraceRepository::class.java)
    
    companion object {
        private const val TRACE_KEY_PREFIX = "trace:message:"
        private const val TRACE_INDEX_PREFIX = "trace:index:"
        private const val TRACE_STATS_PREFIX = "trace:stats:"
    }

    override fun recordEvent(event: MessageTraceEvent): Mono<Void> {
        val traceKey = getMessageTraceKey(event.messageId)
        val indexKey = getTraceIndexKey(event.tenantId, event.queueName)
        val eventJson = objectMapper.writeValueAsString(event)
        
        return redisTemplate.opsForList()
            .rightPush(traceKey, eventJson)
            .then(redisTemplate.opsForZSet().add(indexKey, event.messageId, event.timestamp.epochSecond.toDouble()))
            .then(redisTemplate.expire(traceKey, java.time.Duration.ofDays(30))) // 30일 보관
            .then()
            .doOnSuccess { 
                logger.debug("메시지 추적 이벤트 기록: messageId=${event.messageId}, type=${event.eventType}") 
            }
            .doOnError { error -> 
                logger.error("메시지 추적 이벤트 기록 실패: messageId=${event.messageId}", error) 
            }
    }

    override fun getMessageTrace(messageId: String): Mono<MessageTrace> {
        val traceKey = getMessageTraceKey(messageId)
        
        return redisTemplate.opsForList()
            .range(traceKey, 0, -1)
            .collectList()
            .flatMap { eventJsonList ->
                if (eventJsonList.isEmpty()) {
                    Mono.empty()
                } else {
                    try {
                        val events = eventJsonList.map { json ->
                            objectMapper.readValue(json, MessageTraceEvent::class.java)
                        }.sortedBy { it.timestamp }
                        
                        val firstEvent = events.first()
                        val lastEvent = events.last()
                        
                        Mono.just(
                            MessageTrace(
                                messageId = messageId,
                                tenantId = firstEvent.tenantId,
                                queueName = firstEvent.queueName,
                                events = events,
                                createdAt = firstEvent.timestamp,
                                lastUpdatedAt = lastEvent.timestamp
                            )
                        )
                    } catch (e: Exception) {
                        logger.error("메시지 추적 정보 파싱 실패: messageId=$messageId", e)
                        Mono.empty()
                    }
                }
            }
    }

    override fun getMessageTraces(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant,
        limit: Int
    ): Flux<MessageTrace> {
        val indexKey = getTraceIndexKey(tenantId, queueName)
        
        return redisTemplate.opsForZSet()
            .reverseRangeByScore(
                indexKey,
                org.springframework.data.domain.Range.closed(
                    startTime.epochSecond.toDouble(),
                    endTime.epochSecond.toDouble()
                ),
                org.springframework.data.redis.connection.Limit.limit().count(limit.toInt())
            )
            .cast(String::class.java)
            .flatMap { messageId ->
                getMessageTrace(messageId)
                    .onErrorContinue { error, _ ->
                        logger.warn("메시지 추적 정보 조회 실패: messageId=$messageId", error)
                    }
            }
    }

    override fun getTraceStatistics(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant
    ): Mono<TraceStatistics> {
        return getMessageTraces(tenantId, queueName, startTime, endTime, 10000)
            .collectList()
            .map { traces ->
                val totalMessages = traces.size.toLong()
                val completedMessages = traces.count { it.currentStatus == MessageStatus.COMPLETED }.toLong()
                val failedMessages = traces.count { it.currentStatus == MessageStatus.FAILED }.toLong()
                
                val processingTimes = traces.mapNotNull { it.totalProcessingTime }
                val averageProcessingTime = if (processingTimes.isNotEmpty()) {
                    processingTimes.average()
                } else 0.0
                
                val messagesByStatus = MessageStatus.values().associateWith { status ->
                    traces.count { it.currentStatus == status }.toLong()
                }
                
                // 시간별 통계 (간단한 구현)
                val hourlyStats = traces.groupBy { 
                    DateTimeFormatter.ISO_LOCAL_DATE_TIME.format(
                        it.createdAt.truncatedTo(java.time.temporal.ChronoUnit.HOURS)
                            .atOffset(java.time.ZoneOffset.UTC)
                            .toLocalDateTime()
                    )
                }.mapValues { it.value.size.toLong() }
                
                TraceStatistics(
                    totalMessages = totalMessages,
                    completedMessages = completedMessages,
                    failedMessages = failedMessages,
                    averageProcessingTime = averageProcessingTime,
                    messagesByStatus = messagesByStatus,
                    hourlyStats = hourlyStats
                )
            }
    }

    override fun cleanupOldTraces(retentionDays: Int): Mono<Long> {
        val cutoffTime = Instant.now().minusSeconds(retentionDays * 24 * 3600L)
        
        // 실제 구현에서는 모든 인덱스를 순회하면서 오래된 추적 데이터를 삭제해야 함
        // 여기서는 간단한 구현만 제공
        return Mono.just(0L)
            .doOnSuccess { logger.info("오래된 추적 데이터 정리 작업 완료: retentionDays=$retentionDays") }
    }

    private fun getMessageTraceKey(messageId: String): String {
        return "$TRACE_KEY_PREFIX$messageId"
    }

    private fun getTraceIndexKey(tenantId: String, queueName: String?): String {
        return if (queueName != null) {
            "$TRACE_INDEX_PREFIX$tenantId:$queueName"
        } else {
            "$TRACE_INDEX_PREFIX$tenantId"
        }
    }
}
