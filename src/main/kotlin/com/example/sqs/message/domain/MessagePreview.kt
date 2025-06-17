package com.example.sqs.message.domain

import java.time.Instant

/**
 * 메시지 미리보기 데이터
 * 큐에서 메시지를 제거하지 않고 내용만 조회할 때 사용
 */
data class MessagePreview(
    val messageId: String,
    val messageBody: String,
    val attributes: Map<String, String>,
    val sentTimestamp: Instant,
    val firstReceiveTimestamp: Instant?,
    val receiveCount: Int,
    val approximateFirstReceiveTimestamp: Instant?
) {
    companion object {
        fun fromMessage(message: Message): MessagePreview {
            return MessagePreview(
                messageId = message.messageId,
                messageBody = message.messageBody,
                attributes = message.attributes,
                sentTimestamp = message.sentTimestamp,
                firstReceiveTimestamp = message.firstReceiveTimestamp,
                receiveCount = message.receiveCount,
                approximateFirstReceiveTimestamp = message.firstReceiveTimestamp
            )
        }
    }
}
