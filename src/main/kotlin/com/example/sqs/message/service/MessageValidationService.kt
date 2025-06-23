package com.example.sqs.message.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.common.validation.ValidationRules
import com.example.sqs.message.domain.BatchMessageRequest
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.queue.domain.Queue
import org.springframework.stereotype.Service

/**
 * 메시지 검증 관련 기능을 담당하는 서비스
 */
@Service
class MessageValidationService : BaseReactiveService() {

    override fun getServiceName(): String = "MessageValidationService"

    /**
     * FIFO 큐 메시지 검증
     */
    fun validateFifoMessage(request: SendMessageRequest, queue: Queue) {
        // MessageGroupId는 FIFO 큐에서 필수 (request 필드 또는 attributes에서 확인)
        val messageGroupId = request.messageGroupId
            ?: request.attributes["MessageGroupId"]

        ValidationRules.notBlank(messageGroupId, "MessageGroupId")

        // MessageGroupId 길이 검증 (최대 128자)
        ValidationRules.maxLength(messageGroupId, 128, "MessageGroupId")

        // MessageGroupId는 영숫자, 하이픈, 언더스코어만 허용
        if (messageGroupId != null && !messageGroupId.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
            throw IllegalArgumentException("MessageGroupId는 영숫자, 하이픈(-), 언더스코어(_)만 포함할 수 있습니다")
        }

        // ContentBasedDeduplication이 비활성화된 경우 MessageDeduplicationId 필수
        val contentBasedDeduplication = queue.attributes.contentBasedDeduplication
        if (!contentBasedDeduplication) {
            val messageDeduplicationId = request.messageDeduplicationId
                ?: request.attributes["MessageDeduplicationId"]

            ValidationRules.notBlank(messageDeduplicationId, "MessageDeduplicationId")

            // MessageDeduplicationId 길이 검증 (최대 128자)
            ValidationRules.maxLength(messageDeduplicationId, 128, "MessageDeduplicationId")

            // MessageDeduplicationId는 영숫자, 하이픈, 언더스코어만 허용
            if (messageDeduplicationId != null && !messageDeduplicationId.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
                throw IllegalArgumentException("MessageDeduplicationId는 영숫자, 하이픈(-), 언더스코어(_)만 포함할 수 있습니다")
            }
        }
    }

    /**
     * 배치 메시지 검증
     */
    fun validateBatchMessage(request: BatchMessageRequest, queue: Queue) {
        ValidationRules.notBlank(request.messageBody, "messageBody")
        ValidationRules.notBlank(request.id, "message id")

        if (queue.attributes.fifoQueue) {
            val messageGroupId = request.messageGroupId ?: request.attributes["MessageGroupId"]
            ValidationRules.notBlank(messageGroupId, "MessageGroupId")
        }
    }

    /**
     * 수신 요청 검증
     */
    fun validateReceiveRequest(request: ReceiveMessageRequest, queue: Queue) {
        // FIFO 큐에서는 MessageGroupId가 필수 (선택사항으로 변경 가능)
        if (queue.attributes.fifoQueue && request.messageGroupId.isNullOrBlank()) {
            logger.warn("FIFO 큐에서 MessageGroupId 없이 메시지 수신 요청")
            // 에러 대신 경고로 처리 (선택사항)
        }

        // maxNumberOfMessages 범위 검증
        ValidationRules.inRange(request.maxNumberOfMessages, 1, 10, "maxNumberOfMessages")

        // visibilityTimeoutSeconds 범위 검증
        if (request.visibilityTimeoutSeconds != null) {
            ValidationRules.inRange(request.visibilityTimeoutSeconds, 0, 43200, "visibilityTimeoutSeconds")
        }
    }
}
