package com.example.sqs.message.controller

import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import org.junit.jupiter.api.Test
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.test.web.reactive.server.expectBody

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class MessagePeekIntegrationTest(private val webTestClient: WebTestClient) {

    companion object {
        private const val TEST_API_KEY = "test-api-key"
    }

    @Test
    fun `메시지 미리보기 - 빈 큐`() {
        // 큐 생성
        val queueCreateRequest = CreateQueueRequest(
            queueName = "test-peek-queue",
            attributes = QueueAttributes()
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", TEST_API_KEY)
            .bodyValue(queueCreateRequest)
            .exchange()
            .expectStatus().isCreated

        // 빈 큐에서 메시지 미리보기
        webTestClient.get()
            .uri("/api/v1/queues/test-peek-queue/messages/peek")
            .header("X-API-Key", TEST_API_KEY)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == true)
                val data = body["data"] as List<*>
                assert(data.isEmpty())
            }
    }

    @Test
    fun `메시지 미리보기 - 메시지가 있는 큐`() {
        // 큐 생성
        val queueCreateRequest = CreateQueueRequest(
            queueName = "test-peek-queue-with-messages",
            attributes = QueueAttributes()
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", TEST_API_KEY)
            .bodyValue(queueCreateRequest)
            .exchange()
            .expectStatus().isCreated

        // 메시지 전송
        val messages = listOf("첫 번째 메시지", "두 번째 메시지", "세 번째 메시지")
        
        messages.forEach { messageBody ->
            val sendRequest = SendMessageRequest(
                messageBody = messageBody,
                attributes = mapOf("type" to "test")
            )

            webTestClient.post()
                .uri("/api/v1/queues/test-peek-queue-with-messages/messages")
                .header("X-API-Key", TEST_API_KEY)
                .bodyValue(sendRequest)
                .exchange()
                .expectStatus().isCreated
        }

        // 메시지 미리보기
        webTestClient.get()
            .uri("/api/v1/queues/test-peek-queue-with-messages/messages/peek?maxMessages=2")
            .header("X-API-Key", TEST_API_KEY)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == true)
                
                val data = body["data"] as List<Map<String, Any>>
                assert(data.size == 2) // 최대 2개만 반환
                
                // 첫 번째 메시지 확인 (FIFO 순서)
                val firstMessage = data[0]
                assert(firstMessage["messageBody"] == "첫 번째 메시지")
                assert((firstMessage["attributes"] as Map<String, String>)["type"] == "test")
                
                // 두 번째 메시지 확인
                val secondMessage = data[1]
                assert(secondMessage["messageBody"] == "두 번째 메시지")
            }

        // 미리보기 후에도 메시지가 그대로 있는지 확인 (실제 수신 테스트)
        webTestClient.get()
            .uri("/api/v1/queues/test-peek-queue-with-messages/messages?maxNumberOfMessages=3")
            .header("X-API-Key", TEST_API_KEY)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == true)
                
                val data = body["data"] as List<Map<String, Any>>
                assert(data.size == 3) // 모든 메시지가 여전히 존재
            }
    }

    @Test
    fun `메시지 미리보기 - 존재하지 않는 큐`() {
        webTestClient.get()
            .uri("/api/v1/queues/non-existent-queue/messages/peek")
            .header("X-API-Key", TEST_API_KEY)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == false)
                assert((body["error"] as Map<String, String>)["code"] == "QUEUE_NOT_FOUND")
            }
    }

    @Test
    fun `메시지 미리보기 - 잘못된 API Key`() {
        webTestClient.get()
            .uri("/api/v1/queues/test-queue/messages/peek")
            .header("X-API-Key", "invalid-key")
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == false)
                assert((body["error"] as Map<String, String>)["code"] == "UNAUTHORIZED")
            }
    }

    @Test
    fun `메시지 미리보기 - maxMessages 파라미터 유효성 검증`() {
        // 큐 생성
        val queueCreateRequest = CreateQueueRequest(
            queueName = "test-peek-validation-queue",
            attributes = QueueAttributes()
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", TEST_API_KEY)
            .bodyValue(queueCreateRequest)
            .exchange()
            .expectStatus().isCreated

        // 15개 메시지 전송
        repeat(15) { index ->
            val sendRequest = SendMessageRequest(
                messageBody = "메시지 $index",
                attributes = emptyMap()
            )

            webTestClient.post()
                .uri("/api/v1/queues/test-peek-validation-queue/messages")
                .header("X-API-Key", TEST_API_KEY)
                .bodyValue(sendRequest)
                .exchange()
                .expectStatus().isCreated
        }

        // maxMessages가 10을 초과해도 최대 10개만 반환되는지 확인
        webTestClient.get()
            .uri("/api/v1/queues/test-peek-validation-queue/messages/peek?maxMessages=15")
            .header("X-API-Key", TEST_API_KEY)
            .exchange()
            .expectStatus().isOk
            .expectBody<Map<String, Any>>()
            .consumeWith { response ->
                val body = response.responseBody!!
                assert(body["success"] == true)
                
                val data = body["data"] as List<*>
                assert(data.size == 10) // 최대 10개로 제한
            }
    }
}
