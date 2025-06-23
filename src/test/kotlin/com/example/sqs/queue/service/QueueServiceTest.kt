package com.example.sqs.queue.service

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.test.base.BaseServiceTest
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.test.StepVerifier

/**
 * QueueService 단위 테스트
 */
class QueueServiceTest : BaseServiceTest() {

    @Autowired
    private lateinit var queueService: QueueService

    @Test
    @DisplayName("큐 생성 성공 테스트")
    fun testCreateQueue_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        )
            .expectNextMatches { response ->
                response?.tenantId == tenantId &&
                        response.queueName == queueName &&
                        response.attributes == attributes
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("FIFO 큐 생성 성공 테스트")
    fun testCreateFifoQueue_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "test-fifo-queue.fifo"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3,
            fifoQueue = true,
            contentBasedDeduplication = true
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        )
            .expectNextMatches { response ->
                response?.tenantId == tenantId &&
                        response.queueName == queueName &&
                        response.attributes?.fifoQueue == true
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("중복 큐 생성 실패 테스트")
    fun testCreateDuplicateQueue_Failure() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "duplicate-queue"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
                .flatMap { queueService.createQueue(tenantId, queueName, attributes) }
        )
            .expectError()
            .verify()
    }

    @Test
    @DisplayName("큐 조회 성공 테스트")
    fun testFindQueue_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "find-test-queue"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 60,
            messageRetentionPeriod = 604800,
            maxReceiveCount = 5
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
                .flatMap { queueService.findQueue(tenantId, queueName) }
        )
            .expectNextMatches { queue ->
                queue.tenantId == tenantId &&
                        queue.queueName == queueName &&
                        queue.attributes.visibilityTimeoutSeconds == 60
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("존재하지 않는 큐 조회 실패 테스트")
    fun testFindNonExistentQueue_Failure() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "non-existent-queue"

        // When & Then
        StepVerifier.create(
            queueService.findQueue(tenantId, queueName)
        )
            .expectError()
            .verify()
    }

    @Test
    @DisplayName("큐 목록 조회 테스트")
    fun testListQueues_Success() {
        // Given
        val tenantId = "test-tenant"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        val queueNames = listOf("queue-1", "queue-2", "queue-3")

        // When & Then
        StepVerifier.create(
            reactor.core.publisher.Flux.fromIterable(queueNames)
                .flatMap { queueName ->
                    queueService.createQueue(tenantId, queueName, attributes)
                }
                .then(queueService.listQueues(tenantId, limit = 10, offset = 0, namePattern = null).collectList())
        )
            .expectNextMatches { queues: List<Queue> ->
                queues.size >= 3 &&
                        queues.any { queue -> queue.queueName == "queue-1" } &&
                        queues.any { queue -> queue.queueName == "queue-2" } &&
                        queues.any { queue -> queue.queueName == "queue-3" }
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("큐 삭제 성공 테스트")
    fun testDeleteQueue_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "delete-test-queue"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
                .flatMap { queueService.deleteQueue(tenantId, queueName) }
        )
            .verifyComplete()
    }

    @Test
    @DisplayName("큐 통계 조회 테스트")
    fun testGetQueueStatistics_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "stats-test-queue"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
                .flatMap { queueService.getQueueStatistics(tenantId, queueName) }
        )
            .expectNextMatches { response ->
                response != null &&
                        response.approximateNumberOfMessages >= 0 &&
                        response.approximateNumberOfMessagesNotVisible >= 0
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("큐 속성 업데이트 테스트")
    fun testUpdateQueue_Success() {
        // Given
        val tenantId = "test-tenant"
        val queueName = "update-test-queue"
        val originalAttributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        val updatedAttributes = QueueAttributes(
            visibilityTimeoutSeconds = 60,
            messageRetentionPeriod = 604800,
            maxReceiveCount = 5
        )

        // When & Then
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, originalAttributes)
                .flatMap { queueService.updateQueueAttributes(tenantId, queueName, updatedAttributes) }
        )
            .expectNextMatches { updatedQueue: Queue ->
                updatedQueue.attributes.visibilityTimeoutSeconds == 60 &&
                        updatedQueue.attributes.messageRetentionPeriod == 604800 &&
                        updatedQueue.attributes.maxReceiveCount == 5
            }
            .verifyComplete()
    }

    @Test
    @DisplayName("큐 이름 패턴 검색 테스트")
    fun testListQueuesWithPattern_Success() {
        // Given
        val tenantId = "test-tenant"
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        val queueNames = listOf("test-queue-1", "test-queue-2", "other-queue")

        // When & Then
        StepVerifier.create(
            reactor.core.publisher.Flux.fromIterable(queueNames)
                .flatMap { queueName ->
                    queueService.createQueue(tenantId, queueName, attributes)
                }
                .then(queueService.listQueues(tenantId, limit = 10, offset = 0, namePattern = "test-*").collectList())
        )
            .expectNextMatches { queues: List<Queue> ->
                queues.size == 2 &&
                        queues.all { queue -> queue.queueName.startsWith("test-") }
            }
            .verifyComplete()
    }
}
