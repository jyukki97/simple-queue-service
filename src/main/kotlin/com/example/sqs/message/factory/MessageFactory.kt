package com.example.sqs.message.factory

import com.example.sqs.message.domain.Message
import com.example.sqs.message.service.MessageProcessingService
import org.springframework.stereotype.Component
import java.time.Instant

/**
 * 메시지 객체 생성을 담당하는 팩토리 클래스
 * MessageService와 MessageBatchService에서 공통으로 사용되는 메시지 생성 로직을 분리
 */
@Component
class MessageFactory(
    private val messageProcessingService: MessageProcessingService
) {

    /**
     * 메시지 객체 생성
     * 
     * @param tenantId 테넌트 ID
     * @param targetQueueName 대상 큐 이름
     * @param messageBody 메시지 본문
     * @param attributes 메시지 속성
     * @param delaySeconds 지연 시간 (초)
     * @param messageGroupId 메시지 그룹 ID (FIFO 큐용)
     * @param messageDeduplicationId 메시지 중복 제거 ID (FIFO 큐용)
     * @param compressed 압축 여부
     * @param originalMessageBodyLength 원본 메시지 본문 길이 (압축된 경우)
     */
    fun createMessage(
        tenantId: String,
        targetQueueName: String,
        messageBody: String,
        attributes: Map<String, String>,
        delaySeconds: Int,
        messageGroupId: String?,
        messageDeduplicationId: String?,
        compressed: Boolean,
        originalMessageBodyLength: Int? = null
    ): Message {
        val visibleAt = if (delaySeconds > 0) {
            Instant.now().plusSeconds(delaySeconds.toLong())
        } else {
            Instant.now()
        }

        val enhancedAttributes = messageProcessingService.enhanceMessageAttributes(
            originalAttributes = attributes,
            compressed = compressed,
            originalSize = originalMessageBodyLength,
            partition = targetQueueName
        )

        return Message(
            tenantId = tenantId,
            queueName = targetQueueName,
            messageBody = messageBody,
            attributes = enhancedAttributes,
            sentTimestamp = Instant.now(),
            visibleAt = visibleAt,
            delaySeconds = delaySeconds,
            messageGroupId = messageGroupId ?: attributes["MessageGroupId"],
            messageDeduplicationId = messageDeduplicationId ?: attributes["MessageDeduplicationId"]
        )
    }
}
