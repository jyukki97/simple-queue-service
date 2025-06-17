package com.example.sqs.queue.controller

import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import org.junit.jupiter.api.Test
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.http.MediaType

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class QueueControllerIntegrationTest(
    private val webTestClient: WebTestClient
) {

    @Test
    fun `큐 생성 API 테스트`() {
        val request = CreateQueueRequest(
            queueName = "test-queue",
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.queueName").isEqualTo("test-queue")
            .jsonPath("$.data.tenantId").isEqualTo("tenant-1")
    }

    @Test
    fun `중복 큐 생성시 에러 반환 테스트`() {
        val request = CreateQueueRequest(
            queueName = "duplicate-queue",
            attributes = QueueAttributes()
        )

        // 첫 번째 큐 생성
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated

        // 두 번째 동일한 큐 생성 시도
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated // API Response 구조상 HTTP 201이지만 success: false
            .expectBody()
            .jsonPath("$.success").isEqualTo(false)
            .jsonPath("$.error.code").isEqualTo("QUEUE_ALREADY_EXISTS")
    }

    @Test
    fun `잘못된 API Key로 큐 생성시 에러 반환 테스트`() {
        val request = CreateQueueRequest(
            queueName = "unauthorized-queue",
            attributes = QueueAttributes()
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "invalid-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated // API Response 구조상 HTTP 201이지만 success: false
            .expectBody()
            .jsonPath("$.success").isEqualTo(false)
            .jsonPath("$.error.code").isEqualTo("UNAUTHORIZED")
    }

    @Test
    fun `유효성 검사 실패시 에러 반환 테스트`() {
        val request = mapOf(
            "queueName" to "", // 빈 큐 이름
            "attributes" to mapOf(
                "visibilityTimeoutSeconds" to 30
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isBadRequest
    }

    @Test
    fun `헬스체크 API 테스트`() {
        webTestClient.get()
            .uri("/api/v1/ping")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").isEqualTo("pong")
    }
}
