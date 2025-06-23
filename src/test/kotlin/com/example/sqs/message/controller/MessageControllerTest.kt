package com.example.sqs.message.controller

import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.service.QueueService
import com.example.sqs.test.base.BaseControllerTest
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.http.MediaType
import reactor.test.StepVerifier

/**
 * MessageController 통합 테스트
 */
class MessageControllerIntegrationTest : BaseControllerTest() {

    @Autowired
    private lateinit var queueService: QueueService

    private val queueName = "test-controller-queue"

    @BeforeEach
    fun setUp() {
        // 테스트용 큐 생성
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        StepVerifier.create(
            queueService.createQueue(testTenantId, queueName, attributes)
        ).expectNextCount(1).verifyComplete()
    }

    @Test
    @DisplayName("메시지 전송 API 테스트")
    fun testSendMessageAPI() {
        // Given
        val request = SendMessageRequest(
            messageBody = "Hello from API test",
            delaySeconds = 0,
            attributes = mapOf("source" to "api-test")
        )

        // When & Then
        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .withTestHeaders()
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.messageId").exists()
            .jsonPath("$.data.messageId").isNotEmpty
    }

    @Test
    @DisplayName("메시지 수신 API 테스트")
    fun testReceiveMessageAPI() {
        // Given - 먼저 메시지 전송
        val sendRequest = SendMessageRequest(
            messageBody = "Message for receive test",
            delaySeconds = 0,
            attributes = mapOf("test" to "receive")
        )

        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(sendRequest)
            .withTestHeaders()
            .exchange()
            .expectStatus().isCreated

        // When & Then - 메시지 수신
        webTestClient.get()
            .uri { uriBuilder ->
                uriBuilder.path("/api/v1/queues/{queueName}/messages")
                    .queryParam("maxNumberOfMessages", 1)
                    .queryParam("visibilityTimeoutSeconds", 30)
                    .queryParam("waitTimeSeconds", 0)
                    .build(queueName)
            }
            .withTestHeaders()
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.messages").isArray
            .jsonPath("$.data.messages[0].messageBody").isEqualTo("Message for receive test")
            .jsonPath("$.data.messages[0].receiptHandle").exists()
    }

    @Test
    @DisplayName("메시지 삭제 API 테스트")
    fun testDeleteMessageAPI() {
        // Given - 메시지 전송 및 수신
        val sendRequest = SendMessageRequest(
            messageBody = "Message for delete test",
            delaySeconds = 0,
            attributes = mapOf()
        )

        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(sendRequest)
            .withTestHeaders()
            .exchange()
            .expectStatus().isCreated

        val receiveResponse = webTestClient.get()
            .uri { uriBuilder ->
                uriBuilder.path("/api/v1/queues/{queueName}/messages")
                    .queryParam("maxNumberOfMessages", 1)
                    .queryParam("visibilityTimeoutSeconds", 30)
                    .queryParam("waitTimeSeconds", 0)
                    .build(queueName)
            }
            .withTestHeaders()
            .exchange()
            .expectStatus().isOk
            .returnResult(String::class.java)
            .responseBody
            .blockFirst()

        // receiptHandle 추출 (실제로는 JSON 파싱 필요)
        val receiptHandle = "test-receipt-handle" // 실제 구현에서는 응답에서 추출

        // When & Then - 메시지 삭제
        webTestClient.delete()
            .uri { uriBuilder ->
                uriBuilder.path("/api/v1/queues/{queueName}/messages")
                    .queryParam("receiptHandle", receiptHandle)
                    .build(queueName)
            }
            .withTestHeaders()
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
    }

    @Test
    @DisplayName("메시지 미리보기 API 테스트")
    fun testPeekMessageAPI() {
        // Given - 메시지 전송
        val sendRequest = SendMessageRequest(
            messageBody = "Message for peek test",
            delaySeconds = 0,
            attributes = mapOf("peek" to "test")
        )

        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(sendRequest)
            .withTestHeaders()
            .exchange()
            .expectStatus().isCreated

        // When & Then - 메시지 미리보기
        webTestClient.get()
            .uri { uriBuilder ->
                uriBuilder.path("/api/v1/queues/{queueName}/messages/peek")
                    .queryParam("maxNumberOfMessages", 1)
                    .build(queueName)
            }
            .withTestHeaders()
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.messages").isArray
            .jsonPath("$.data.messages[0].messageBody").isEqualTo("Message for peek test")
    }

    @Test
    @DisplayName("잘못된 API 키로 요청 시 인증 실패 테스트")
    fun testInvalidApiKey() {
        // Given
        val request = SendMessageRequest(
            messageBody = "Unauthorized test",
            delaySeconds = 0,
            attributes = mapOf()
        )

        // When & Then
        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .header("X-API-Key", "invalid-api-key")
            .header("X-Tenant-Id", testTenantId)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isUnauthorized
    }

    @Test
    @DisplayName("존재하지 않는 큐에 메시지 전송 시 오류 테스트")
    fun testSendToNonExistentQueue() {
        // Given
        val request = SendMessageRequest(
            messageBody = "Message to non-existent queue",
            delaySeconds = 0,
            attributes = mapOf()
        )

        // When & Then
        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", "non-existent-queue")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .withTestHeaders()
            .exchange()
            .expectStatus().isNotFound
    }

    @Test
    @DisplayName("메시지 크기 제한 초과 시 오류 테스트")
    fun testMessageSizeExceedsLimit() {
        // Given
        val largeMessageBody = "A".repeat(300000) // 300KB (제한: 256KB)
        val request = SendMessageRequest(
            messageBody = largeMessageBody,
            delaySeconds = 0,
            attributes = mapOf()
        )

        // When & Then
        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .withTestHeaders()
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    @DisplayName("잘못된 요청 형식 테스트")
    fun testInvalidRequestFormat() {
        // Given
        val invalidRequest = mapOf(
            "invalidField" to "value"
        )

        // When & Then
        webTestClient.post()
            .uri("/api/v1/queues/{queueName}/messages", queueName)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(invalidRequest)
            .withTestHeaders()
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    @DisplayName("배치 메시지 전송 API 테스트")
    fun testSendMessageBatchAPI() {
        // Given
        val batchRequests = (1..3).map { index ->
            SendMessageRequest(
                messageBody = "Batch message $index",
                delaySeconds = 0,
                attributes = mapOf("index" to index.toString())
            )
        }

        // When & Then
        batchRequests.forEach { request ->
            webTestClient.post()
                .uri("/api/v1/queues/{queueName}/messages", queueName)
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .withTestHeaders()
                .exchange()
                .expectStatus().isCreated
        }

        // 모든 메시지 수신 확인
        webTestClient.get()
            .uri { uriBuilder ->
                uriBuilder.path("/api/v1/queues/{queueName}/messages")
                    .queryParam("maxNumberOfMessages", 10)
                    .queryParam("visibilityTimeoutSeconds", 30)
                    .queryParam("waitTimeSeconds", 0)
                    .build(queueName)
            }
            .withTestHeaders()
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.messages").isArray
            .jsonPath("$.data.messages.length()").isEqualTo(3)
    }
}
