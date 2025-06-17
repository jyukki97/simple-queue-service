package com.example.sqs.tracing.service

import com.example.sqs.tracing.domain.*
import com.example.sqs.tracing.repository.MessageTraceRepository
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

/**
 * 메시지 추적 서비스
 */
@Service
class MessageTraceService(
    private val messageTraceRepository: MessageTraceRepository
) {

    private val logger = LoggerFactory.getLogger(MessageTraceService::class.java)

    /**
     * 메시지 전송 이벤트를 기록합니다
     */
    fun recordMessageSent(
        messageId: String,
        tenantId: String,
        queueName: String,
        traceId: String? = null,
        metadata: Map<String, Any> = emptyMap()
    ): Mono<Void> {
        val event = MessageTraceEvent(
            eventId = UUID.randomUUID().toString(),
            messageId = messageId,
            tenantId = tenantId,
            queueName = queueName,
            eventType = MessageTraceEventType.SENT,
            timestamp = Instant.now(),
            metadata = metadata,
            traceId = traceId,
            spanId = UUID.randomUUID().toString()
        )
        
        return messageTraceRepository.recordEvent(event)
    }

    /**
     * 메시지 수신 이벤트를 기록합니다
     */
    fun recordMessageReceived(
        messageId: String,
        tenantId: String,
        queueName: String,
        receiptHandle: String,
        traceId: String? = null,
        metadata: Map<String, Any> = emptyMap()
    ): Mono<Void> {
        val event = MessageTraceEvent(
            eventId = UUID.randomUUID().toString(),
            messageId = messageId,
            tenantId = tenantId,
            queueName = queueName,
            eventType = MessageTraceEventType.RECEIVED,
            timestamp = Instant.now(),
            metadata = metadata + ("receiptHandle" to receiptHandle),
            traceId = traceId,
            spanId = UUID.randomUUID().toString()
        )
        
        return messageTraceRepository.recordEvent(event)
    }

    /**
     * 메시지 삭제 이벤트를 기록합니다
     */
    fun recordMessageDeleted(
        messageId: String,
        tenantId: String,
        queueName: String,
        receiptHandle: String,
        traceId: String? = null,
        metadata: Map<String, Any> = emptyMap()
    ): Mono<Void> {
        val event = MessageTraceEvent(
            eventId = UUID.randomUUID().toString(),
            messageId = messageId,
            tenantId = tenantId,
            queueName = queueName,
            eventType = MessageTraceEventType.DELETED,
            timestamp = Instant.now(),
            metadata = metadata + ("receiptHandle" to receiptHandle),
            traceId = traceId,
            spanId = UUID.randomUUID().toString()
        )
        
        return messageTraceRepository.recordEvent(event)
    }

    /**
     * 메시지 미리보기 이벤트를 기록합니다
     */
    fun recordMessagePeeked(
        messageId: String,
        tenantId: String,
        queueName: String,
        traceId: String? = null,
        metadata: Map<String, Any> = emptyMap()
    ): Mono<Void> {
        val event = MessageTraceEvent(
            eventId = UUID.randomUUID().toString(),
            messageId = messageId,
            tenantId = tenantId,
            queueName = queueName,
            eventType = MessageTraceEventType.PEEKED,
            timestamp = Instant.now(),
            metadata = metadata,
            traceId = traceId,
            spanId = UUID.randomUUID().toString()
        )
        
        return messageTraceRepository.recordEvent(event)
    }

    /**
     * DLQ 이동 이벤트를 기록합니다
     */
    fun recordMessageMovedToDLQ(
        messageId: String,
        tenantId: String,
        queueName: String,
        dlqName: String,
        reason: String,
        traceId: String? = null,
        metadata: Map<String, Any> = emptyMap()
    ): Mono<Void> {
        val event = MessageTraceEvent(
            eventId = UUID.randomUUID().toString(),
            messageId = messageId,
            tenantId = tenantId,
            queueName = queueName,
            eventType = MessageTraceEventType.MOVED_TO_DLQ,
            timestamp = Instant.now(),
            metadata = metadata + mapOf("dlqName" to dlqName, "reason" to reason),
            traceId = traceId,
            spanId = UUID.randomUUID().toString()
        )
        
        return messageTraceRepository.recordEvent(event)
    }

    /**
     * 메시지의 전체 추적 정보를 조회합니다
     */
    fun getMessageTrace(messageId: String): Mono<MessageTrace> {
        return messageTraceRepository.getMessageTrace(messageId)
    }

    /**
     * 특정 기간의 메시지 추적 정보를 조회합니다
     */
    fun getMessageTraces(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant,
        limit: Int = 100
    ): Flux<MessageTrace> {
        return messageTraceRepository.getMessageTraces(tenantId, queueName, startTime, endTime, limit)
    }

    /**
     * 추적 통계를 조회합니다
     */
    fun getTraceStatistics(
        tenantId: String,
        queueName: String?,
        startTime: Instant,
        endTime: Instant
    ): Mono<TraceStatistics> {
        return messageTraceRepository.getTraceStatistics(tenantId, queueName, startTime, endTime)
    }

    /**
     * 분산 추적 ID를 생성합니다
     */
    fun generateTraceId(): String {
        return UUID.randomUUID().toString().replace("-", "")
    }
}
