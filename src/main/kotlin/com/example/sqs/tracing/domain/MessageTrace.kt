package com.example.sqs.tracing.domain

import java.time.Instant

/**
 * 메시지 추적 이벤트 타입
 */
enum class MessageTraceEventType {
    SENT,           // 메시지 전송
    RECEIVED,       // 메시지 수신
    DELETED,        // 메시지 삭제
    MOVED_TO_DLQ,   // DLQ로 이동
    VISIBILITY_TIMEOUT_EXPIRED, // 가시성 타임아웃 만료
    PEEKED,         // 메시지 미리보기
    PURGED          // 큐 퍼지로 인한 삭제
}

/**
 * 메시지 추적 이벤트
 */
data class MessageTraceEvent(
    val eventId: String,
    val messageId: String,
    val tenantId: String,
    val queueName: String,
    val eventType: MessageTraceEventType,
    val timestamp: Instant,
    val metadata: Map<String, Any> = emptyMap(),
    val traceId: String? = null,  // 분산 추적 ID
    val spanId: String? = null    // 스팬 ID
)

/**
 * 메시지 추적 정보 (전체 생명 주기)
 */
data class MessageTrace(
    val messageId: String,
    val tenantId: String,
    val queueName: String,
    val events: List<MessageTraceEvent>,
    val createdAt: Instant,
    val lastUpdatedAt: Instant
) {
    val totalProcessingTime: Long?
        get() {
            val sentEvent = events.find { it.eventType == MessageTraceEventType.SENT }
            val completedEvent = events.find { 
                it.eventType == MessageTraceEventType.DELETED || 
                it.eventType == MessageTraceEventType.MOVED_TO_DLQ 
            }
            
            return if (sentEvent != null && completedEvent != null) {
                completedEvent.timestamp.epochSecond - sentEvent.timestamp.epochSecond
            } else null
        }
    
    val currentStatus: MessageStatus
        get() {
            val lastEvent = events.maxByOrNull { it.timestamp }
            return when (lastEvent?.eventType) {
                MessageTraceEventType.SENT -> MessageStatus.PENDING
                MessageTraceEventType.RECEIVED -> MessageStatus.IN_FLIGHT
                MessageTraceEventType.DELETED -> MessageStatus.COMPLETED
                MessageTraceEventType.MOVED_TO_DLQ -> MessageStatus.FAILED
                MessageTraceEventType.PURGED -> MessageStatus.PURGED
                else -> MessageStatus.UNKNOWN
            }
        }
}

/**
 * 메시지 상태
 */
enum class MessageStatus {
    PENDING,    // 대기 중
    IN_FLIGHT,  // 처리 중
    COMPLETED,  // 완료
    FAILED,     // 실패 (DLQ 이동)
    PURGED,     // 퍼지됨
    UNKNOWN     // 알 수 없음
}

/**
 * 추적 통계
 */
data class TraceStatistics(
    val totalMessages: Long,
    val completedMessages: Long,
    val failedMessages: Long,
    val averageProcessingTime: Double,
    val messagesByStatus: Map<MessageStatus, Long>,
    val hourlyStats: Map<String, Long> // 시간별 처리량
)
