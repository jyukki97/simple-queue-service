package com.example.sqs.message.domain

import java.time.Instant
import java.util.*

data class Message(
    val messageId: String = UUID.randomUUID().toString(),
    val tenantId: String,
    val queueName: String,
    val messageBody: String,
    val attributes: Map<String, String> = emptyMap(),
    val sentTimestamp: Instant,
    val firstReceiveTimestamp: Instant? = null,
    val receiveCount: Int = 0,
    val visibleAt: Instant,
    val receiptHandle: String? = null
) {
    init {
        require(messageBody.toByteArray().size <= 262144) { "Message body cannot exceed 256KB" }
        require(tenantId.isNotBlank()) { "Tenant ID cannot be blank" }
        require(queueName.isNotBlank()) { "Queue name cannot be blank" }
    }
    
    fun getMessageKey(): String = "message:${messageId}"
    
    fun isVisible(now: Instant = Instant.now()): Boolean = visibleAt <= now
    
    fun withIncrementedReceiveCount(): Message = copy(
        receiveCount = receiveCount + 1,
        firstReceiveTimestamp = firstReceiveTimestamp ?: Instant.now()
    )
    
    fun withNewReceiptHandle(handle: String, visibilityTimeout: Int): Message = copy(
        receiptHandle = handle,
        visibleAt = Instant.now().plusSeconds(visibilityTimeout.toLong())
    )
}

// DTO classes for API requests/responses
data class SendMessageRequest(
    val messageBody: String,
    val delaySeconds: Int = 0,
    val attributes: Map<String, String> = emptyMap()
) {
    init {
        require(messageBody.isNotBlank()) { "Message body cannot be blank" }
        require(delaySeconds in 0..900) { "Delay seconds must be between 0 and 900" }
    }
}

data class BatchMessageRequest(
    val id: String,
    val messageBody: String,
    val delaySeconds: Int = 0,
    val attributes: Map<String, String> = emptyMap()
)

data class SendMessageBatchRequest(
    val messages: List<BatchMessageRequest>
) {
    init {
        require(messages.isNotEmpty()) { "Messages cannot be empty" }
        require(messages.size <= 10) { "Cannot send more than 10 messages in a batch" }
    }
}

data class ReceiveMessageRequest(
    val maxNumberOfMessages: Int = 1,
    val visibilityTimeoutSeconds: Int? = null,
    val waitTimeSeconds: Int = 0
) {
    init {
        require(maxNumberOfMessages in 1..10) { "Max number of messages must be between 1 and 10" }
        require(waitTimeSeconds in 0..20) { "Wait time seconds must be between 0 and 20" }
    }
}

data class MessageResponse(
    val messageId: String,
    val messageBody: String,
    val attributes: Map<String, String>,
    val receiptHandle: String,
    val sentTimestamp: Instant
)
