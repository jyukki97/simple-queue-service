package com.example.sqs.tracing.repository

import com.example.sqs.tracing.domain.MessageTrace
import com.example.sqs.tracing.domain.MessageTraceEvent
import com.example.sqs.tracing.domain.TraceStatistics
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

/**
 * 메시지 추적 레포지토리 인터페이스
 */
interface MessageTraceRepository {
    
    /**
     * 메시지 추적 이벤트를 기록합니다
     */
    fun recordEvent(event: MessageTraceEvent): Mono<Void>
    
    /**
     * 메시지의 전체 추적 정보를 조회합니다
     */
    fun getMessageTrace(messageId: String): Mono<MessageTrace>
    
    /**
     * 특정 기간 동안의 메시지 추적 정보를 조회합니다
     */
    fun getMessageTraces(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant,
        limit: Int = 100
    ): Flux<MessageTrace>
    
    /**
     * 추적 통계를 계산합니다
     */
    fun getTraceStatistics(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant
    ): Mono<TraceStatistics>
    
    /**
     * 오래된 추적 데이터를 삭제합니다
     */
    fun cleanupOldTraces(retentionDays: Int): Mono<Long>
}
