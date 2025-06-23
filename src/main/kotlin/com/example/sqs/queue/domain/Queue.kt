package com.example.sqs.queue.domain

import java.time.Instant

data class Queue(
    val tenantId: String,
    val queueName: String,
    val attributes: QueueAttributes,
    val createdAt: Instant,
    val updatedAt: Instant
) {
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
    val redrivePolicy: RedrivePolicy? = null,
    val fifoQueue: Boolean = false, // FIFO 큐 여부
    val contentBasedDeduplication: Boolean = false, // 콘텐츠 기반 중복 제거
    val receiveMessageWaitTimeSeconds: Int = 0 // 롱 폴링 대기 시간
) {
    init {
        require(visibilityTimeoutSeconds in 0..43200) { "가시성 타임아웃은 0~43200초 사이여야 합니다" }
        require(messageRetentionPeriod in 60..1209600) { "메시지 보존 기간은 60~1209600초 사이여야 합니다" }
        require(delaySeconds in 0..900) { "지연 시간은 0~900초 사이여야 합니다" }
        require(maxReceiveCount in 1..1000) { "최대 수신 횟수는 1~1000 사이여야 합니다" }
        require(receiveMessageWaitTimeSeconds in 0..20) { "롱 폴링 대기 시간은 0~20초 사이여야 합니다" }
    }
}

data class RedrivePolicy(
    val deadLetterTargetArn: String,
    val maxReceiveCount: Int
)

// API 요청/응답용 DTO 클래스들
data class CreateQueueRequest(
    val queueName: String,
    val attributes: QueueAttributes = QueueAttributes()
) {
    init {
        require(queueName.isNotBlank()) { "큐 이름은 비어있을 수 없습니다" }
        require(queueName.matches(Regex("^[a-zA-Z0-9._-]+$"))) { "큐 이름은 영숫자, 점, 하이픈, 언더스코어만 사용할 수 있습니다" }
        if (attributes.fifoQueue) {
            require(queueName.endsWith(".fifo")) { "FIFO 큐 이름은 .fifo로 끝나야 합니다" }
        }
    }
}

data class UpdateQueueRequest(
    val attributes: QueueAttributes
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

/**
 * 큐 목록 응답 데이터 클래스
 */
data class QueueListResponse(
    val queues: List<Queue>,
    val nextPageToken: String?
)

