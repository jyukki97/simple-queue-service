package com.example.sqs.streaming.domain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant

@JsonIgnoreProperties(ignoreUnknown = true)
data class StreamingMessage(
    val messageId: String,
    val tenantId: String,
    val queueName: String,
    val messageBody: String,
    val attributes: Map<String, String> = emptyMap(),
    val timestamp: Instant = Instant.now(),
    val messageGroupId: String? = null,
    val messageType: String = "MESSAGE",
    val sequenceNumber: Long? = null
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StreamingEvent(
    val eventId: String,
    val eventType: StreamingEventType,
    val tenantId: String,
    val queueName: String,
    val timestamp: Instant = Instant.now(),
    val data: Any
)

enum class StreamingEventType {
    MESSAGE_SENT,
    MESSAGE_RECEIVED,
    MESSAGE_DELETED,
    QUEUE_CREATED,
    QUEUE_DELETED,
    QUEUE_PURGED,
    CONNECTION_ESTABLISHED,
    CONNECTION_LOST,
    HEARTBEAT
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class StreamingSubscription(
    val subscriptionId: String,
    val tenantId: String,
    val queueNames: Set<String> = emptySet(),
    val eventTypes: Set<StreamingEventType> = setOf(StreamingEventType.MESSAGE_SENT),
    val filterExpression: String? = null,
    val batchSize: Int = 1,
    val maxWaitTime: Long = 10000, // milliseconds
    val timestamp: Instant = Instant.now()
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StreamingConnection(
    val connectionId: String,
    val tenantId: String,
    val sessionId: String,
    val subscriptions: MutableSet<StreamingSubscription> = mutableSetOf(),
    val connectedAt: Instant = Instant.now(),
    val lastHeartbeat: Instant = Instant.now(),
    val isActive: Boolean = true
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class SubscribeRequest(
    val queueNames: Set<String> = emptySet(),
    val eventTypes: Set<StreamingEventType> = setOf(StreamingEventType.MESSAGE_SENT),
    val filterExpression: String? = null,
    val batchSize: Int = 1,
    val maxWaitTime: Long = 10000
) {
    init {
        require(batchSize in 1..100) { "배치 크기는 1~100 사이여야 합니다" }
        require(maxWaitTime in 1000..60000) { "최대 대기 시간은 1~60초 사이여야 합니다" }
    }
}

@JsonIgnoreProperties(ignoreUnknown = true)
data class UnsubscribeRequest(
    val subscriptionId: String
)

@JsonIgnoreProperties(ignoreUnknown = true)
data class StreamingResponse(
    val type: String,
    val success: Boolean,
    val data: Any? = null,
    val error: String? = null,
    val timestamp: Instant = Instant.now()
)