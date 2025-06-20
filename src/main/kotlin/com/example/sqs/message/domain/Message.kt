package com.example.sqs.message.domain

import com.fasterxml.jackson.annotation.JsonIgnoreProperties
import java.time.Instant
import java.util.*

@JsonIgnoreProperties(ignoreUnknown = true)
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
    val receiptHandle: String? = null,
    val delaySeconds: Int = 0
) {
    init {
        require(messageBody.toByteArray().size <= 262144) { "메시지 크기는 256KB를 초과할 수 없습니다" }
        require(tenantId.isNotBlank()) { "테넌트 ID는 비어있을 수 없습니다" }
        require(queueName.isNotBlank()) { "큐 이름은 비어있을 수 없습니다" }
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
        require(messageBody.isNotBlank()) { "메시지 본문은 비어있을 수 없습니다" }
        require(delaySeconds in 0..900) { "지연 시간은 0~900초 사이여야 합니다" }
    }
}

data class SendMessageResponse(
    val messageId: String,
    val messageBodyMD5: String? = null
)

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
        require(messages.isNotEmpty()) { "메시지 목록은 비어있을 수 없습니다" }
        require(messages.size <= 10) { "한 번에 10개 이상의 메시지를 전송할 수 없습니다" }
    }
}

data class ReceiveMessageRequest(
    val maxNumberOfMessages: Int = 1,
    val visibilityTimeoutSeconds: Int? = null,
    val waitTimeSeconds: Int = 0
) {
    init {
        require(maxNumberOfMessages in 1..10) { "최대 메시지 수는 1~10개 사이여야 합니다" }
        require(waitTimeSeconds in 0..20) { "대기 시간은 0~20초 사이여야 합니다" }
    }
}

data class MessageResponse(
    val messageId: String,
    val messageBody: String,
    val attributes: Map<String, String>,
    val receiptHandle: String,
    val sentTimestamp: Instant
)

data class BatchMessageResponse(
    val id: String,
    val messageId: String,
    val messageBodyMD5: String? = null
)

data class SendMessageBatchResponse(
    val successful: List<BatchMessageResponse>,
    val failed: List<BatchMessageFailure> = emptyList()
)

data class BatchMessageFailure(
    val id: String,
    val code: String,
    val message: String,
    val senderFault: Boolean = true
)

data class DeleteMessageBatchRequest(
    val receiptHandles: List<String>
) {
    init {
        require(receiptHandles.isNotEmpty()) { "Receipt handle 목록은 비어있을 수 없습니다" }
        require(receiptHandles.size <= 10) { "한 번에 10개 이상의 메시지를 삭제할 수 없습니다" }
    }
}

data class DeleteMessageBatchResponse(
    val successful: List<String>,
    val failed: List<BatchMessageFailure> = emptyList()
)

data class ReceiveMessageResponse(
    val messages: List<MessageResponse>
)

data class PeekMessageResponse(
    val messages: List<MessagePreview>
)
