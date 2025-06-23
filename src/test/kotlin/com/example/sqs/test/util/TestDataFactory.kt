package com.example.sqs.test.util

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.tenant.domain.Tenant
import java.time.Instant
import java.util.*

/**
 * 테스트용 데이터 생성 유틸리티
 */
object TestDataFactory {

    /**
     * 고유한 큐 이름 생성
     */
    fun createUniqueQueueName(prefix: String = "test-queue"): String {
        return "$prefix-${System.currentTimeMillis()}-${(Math.random() * 1000).toInt()}"
    }

    /**
     * 테스트용 Queue 생성
     */
    fun createTestQueue(
        tenantId: String = "test-tenant",
        queueName: String = createUniqueQueueName(),
        fifoQueue: Boolean = false
    ): Queue {
        return Queue(
            tenantId = tenantId,
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                maxReceiveCount = 3,
                fifoQueue = fifoQueue,
                contentBasedDeduplication = fifoQueue
            ),
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
    }

    /**
     * 테스트용 Message 생성
     */
    fun createTestMessage(
        tenantId: String = "test-tenant",
        queueName: String = createUniqueQueueName(),
        messageBody: String = "test message body",
        messageGroupId: String? = null
    ): Message {
        return Message(
            messageId = UUID.randomUUID().toString(),
            tenantId = tenantId,
            queueName = queueName,
            messageBody = messageBody,
            attributes = mapOf(),
            sentTimestamp = Instant.now(),
            visibleAt = Instant.now(),
            receiptHandle = null,
            receiveCount = 0,
            messageGroupId = messageGroupId,
            messageDeduplicationId = if (messageGroupId != null) UUID.randomUUID().toString() else null
        )
    }

    /**
     * 테스트용 Tenant 생성
     */
    fun createTestTenant(
        tenantId: String = "test-tenant",
        apiKey: String = "test-api-key",
        name: String = "Test Tenant"
    ): Tenant {
        return Tenant(
            tenantId = tenantId,
            apiKey = apiKey,
            name = name,
            createdAt = Instant.now()
        )
    }

    /**
     * 대용량 메시지 바디 생성
     */
    fun createLargeMessageBody(sizeInBytes: Int): String {
        return "A".repeat(sizeInBytes)
    }

    /**
     * 배치 메시지 생성
     */
    fun createBatchMessages(
        count: Int,
        tenantId: String = "test-tenant",
        queueName: String = createUniqueQueueName(),
        messageBodyPrefix: String = "batch message"
    ): List<Message> {
        return (1..count).map { index ->
            createTestMessage(
                tenantId = tenantId,
                queueName = queueName,
                messageBody = "$messageBodyPrefix $index"
            )
        }
    }
}

/**
 * 테스트용 어서션 유틸리티
 */
object TestAssertions {

    /**
     * Queue 동등성 검증
     */
    fun assertQueuesEqual(expected: Queue, actual: Queue) {
        assert(expected.tenantId == actual.tenantId) { "Tenant ID mismatch" }
        assert(expected.queueName == actual.queueName) { "Queue name mismatch" }
        assert(expected.attributes == actual.attributes) { "Attributes mismatch" }
    }

    /**
     * Message 동등성 검증
     */
    fun assertMessagesEqual(expected: Message, actual: Message) {
        assert(expected.tenantId == actual.tenantId) { "Tenant ID mismatch" }
        assert(expected.queueName == actual.queueName) { "Queue name mismatch" }
        assert(expected.messageBody == actual.messageBody) { "Message body mismatch" }
        assert(expected.attributes == actual.attributes) { "Attributes mismatch" }
    }

    /**
     * 메트릭 값 범위 검증
     */
    fun assertMetricInRange(metricName: String, actual: Double, min: Double, max: Double) {
        assert(actual in min..max) {
            "Metric $metricName value $actual is not in range [$min, $max]"
        }
    }
}
