package com.example.sqs.queue.domain

import java.time.Instant

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
)

data class RedrivePolicy(
    val deadLetterTargetArn: String,
    val maxReceiveCount: Int
)

// DTO classes for API requests/responses
data class CreateQueueRequest(
    val queueName: String,
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
