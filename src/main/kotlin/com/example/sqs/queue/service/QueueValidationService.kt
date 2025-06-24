package com.example.sqs.queue.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.common.validation.ValidationRules
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import org.springframework.stereotype.Service

/**
 * 큐 검증 관련 기능을 담당하는 서비스
 */
@Service
class QueueValidationService : BaseReactiveService() {

    override fun getServiceName(): String = "QueueValidationService"

    /**
     * 큐 생성 요청을 검증합니다
     */
    fun validateQueueCreationRequest(tenantId: String, queueName: String, attributes: QueueAttributes) {
        validateTenantId(tenantId)
        validateQueueName(queueName)
        validateQueueAttributes(attributes)

        // FIFO 큐 특별 검증
        if (attributes.fifoQueue) {
            validateFifoQueueCreation(queueName, attributes)
        }
    }

    /**
     * 큐 속성 업데이트를 검증합니다
     */
    fun validateQueueAttributesUpdate(existingQueue: Queue, newAttributes: QueueAttributes) {
        validateQueueAttributes(newAttributes)

        // 변경 불가능한 속성 검증
        validateImmutableAttributes(existingQueue, newAttributes)

        // FIFO 큐 속성 변경 검증
        if (existingQueue.attributes.fifoQueue || newAttributes.fifoQueue) {
            validateFifoAttributeChanges(existingQueue, newAttributes)
        }
    }

    /**
     * 테넌트 ID 검증
     */
    private fun validateTenantId(tenantId: String) {
        ValidationRules.notBlank(tenantId, "tenantId")
        ValidationRules.maxLength(tenantId, 128, "tenantId")

        // 테넌트 ID 형식 검증 (영숫자, 하이픈, 언더스코어만 허용)
        if (!tenantId.matches(Regex("^[a-zA-Z0-9_-]+$"))) {
            throw IllegalArgumentException("tenantId는 영숫자, 하이픈(-), 언더스코어(_)만 포함할 수 있습니다")
        }
    }

    /**
     * 큐 이름 검증
     */
    private fun validateQueueName(queueName: String) {
        ValidationRules.notBlank(queueName, "queueName")
        ValidationRules.minLength(queueName, 1, "queueName")
        ValidationRules.maxLength(queueName, 80, "queueName")

        // 큐 이름 형식 검증
        if (!queueName.matches(Regex("^[a-zA-Z0-9._-]+$"))) {
            throw IllegalArgumentException("queueName은 영숫자, 하이픈(-), 언더스코어(_) 및 점(.)만 포함할 수 있습니다")
        }

        // FIFO 큐 이름 규칙 (선택사항)
        if (queueName.endsWith(".fifo") && queueName.length > 80) {
            throw IllegalArgumentException("FIFO 큐 이름은 .fifo 접미사를 포함하여 80자를 초과할 수 없습니다")
        }
    }

    /**
     * 큐 속성 검증
     */
    private fun validateQueueAttributes(attributes: QueueAttributes) {
        // 기본 속성 검증
        attributes.visibilityTimeoutSeconds.let {
            ValidationRules.inRange(it, 0, 43200, "visibilityTimeoutSeconds")
        }

        attributes.messageRetentionPeriod.let { period ->
            ValidationRules.inRange(period, 60, 1209600, "messageRetentionPeriod")
        }

        attributes.maxReceiveCount.let {
            ValidationRules.positive(it, "maxReceiveCount")
        }

        attributes.delaySeconds.let {
            ValidationRules.inRange(it, 0, 900, "delaySeconds")
        }

        attributes.receiveMessageWaitTimeSeconds.let {
            ValidationRules.inRange(it, 0, 20, "receiveMessageWaitTimeSeconds")
        }
    }

    /**
     * FIFO 큐 생성 검증
     */
    private fun validateFifoQueueCreation(queueName: String, attributes: QueueAttributes) {
        // FIFO 큐는 .fifo로 끝나야 함 (AWS SQS 규칙)
        if (!queueName.endsWith(".fifo")) {
            logger.warn("FIFO 큐 이름이 .fifo로 끝나지 않습니다: $queueName")
        }

        // FIFO 큐에서 delaySeconds는 지원되지 않음
        if (attributes.delaySeconds > 0) {
            throw IllegalArgumentException("FIFO 큐에서는 delaySeconds를 설정할 수 없습니다")
        }
    }

    /**
     * 변경 불가능한 속성 검증
     */
    private fun validateImmutableAttributes(existingQueue: Queue, newAttributes: QueueAttributes) {
        // FIFO 큐 여부는 변경 불가
        if (existingQueue.attributes.fifoQueue != newAttributes.fifoQueue) {
            throw IllegalArgumentException("큐의 FIFO 설정은 생성 후 변경할 수 없습니다")
        }

        // ContentBasedDeduplication도 FIFO 큐에서만 설정 가능
        if (!existingQueue.attributes.fifoQueue && newAttributes.contentBasedDeduplication) {
            throw IllegalArgumentException("ContentBasedDeduplication은 FIFO 큐에서만 사용할 수 있습니다")
        }
    }

    /**
     * FIFO 큐 속성 변경 검증
     */
    private fun validateFifoAttributeChanges(@Suppress("UNUSED_PARAMETER") existingQueue: Queue, newAttributes: QueueAttributes) {
        // FIFO 큐에서 변경 가능한 속성들만 허용
        if (newAttributes.delaySeconds > 0) {
            throw IllegalArgumentException("FIFO 큐에서는 delaySeconds를 설정할 수 없습니다")
        }
    }
}
