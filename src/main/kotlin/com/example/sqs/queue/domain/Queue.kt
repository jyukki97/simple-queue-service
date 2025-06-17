package com.example.sqs.queue.domain

import java.time.Instant
import jakarta.validation.Valid
import jakarta.validation.constraints.NotBlank
import jakarta.validation.constraints.Pattern

data class Queue(
    val tenantId: String,
    val queueName: String,
    val attributes: QueueAttributes,
    val createdAt: Instant,
    val updatedAt: Instant
) {
    fun getQueueKey(): String = "queue:${tenantId}:${queueName}"
    fun getMessagesKey(): String = "queue:messages:${tenantId}:${queueName}"
    fun getDelayedKey(): String = "queue:delayed:${tenantId}:${queueName}"
    fun getInFlightKey(): String = "queue:inflight:${tenantId}:${queueName}"
}

data class QueueAttributes(
    val visibilityTimeoutSeconds: Int = 30,
    val messageRetentionPeriod: Int = 1209600, // 14일
    val delaySeconds: Int = 0,
    val maxReceiveCount: Int = 3,
    val deadLetterTargetArn: String? = null,
    val redrivePolicy: RedrivePolicy? = null
) {
    init {
        require(visibilityTimeoutSeconds in 0..43200) { "Visibility timeout must be between 0 and 43200 seconds" }
        require(messageRetentionPeriod in 60..1209600) { "Message retention period must be between 60 and 1209600 seconds" }
        require(delaySeconds in 0..900) { "Delay seconds must be between 0 and 900 seconds" }
        require(maxReceiveCount in 1..1000) { "Max receive count must be between 1 and 1000" }
    }
}

data class RedrivePolicy(
    val deadLetterTargetArn: String,
    val maxReceiveCount: Int
)

// DTO classes for API requests/responses
data class CreateQueueRequest(
    @field:NotBlank(message = "Queue name cannot be blank")
    @field:Pattern(regexp = "^[a-zA-Z0-9_-]+$", message = "Queue name can only contain alphanumeric characters, hyphens, and underscores")
    val queueName: String,
    
    @field:Valid
    val attributes: QueueAttributes = QueueAttributes()
)

data class ListQueuesRequest(
    val limit: Int = 20,
    val offset: Int = 0,
    val namePattern: String? = null
)

data class QueueStatsResponse(
    val availableMessages: Int,
    val inFlightMessages: Int,
    val delayedMessages: Int,
    val deadLetterMessages: Int
)
