package com.example.sqs.queue.controller

import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.UpdateQueueRequest
import com.example.sqs.common.domain.ApiResponse
import org.junit.jupiter.api.Test
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.web.reactive.server.WebTestClient
import org.springframework.http.MediaType

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
class QueueManagementIntegrationTest(
    private val webTestClient: WebTestClient
) {

    @Test
    fun `큐 생성-조회-삭제 전체 플로우 테스트`() {
        val queueName = "full-flow-test-queue"
        val request = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 60,
                messageRetentionPeriod = 600,
                delaySeconds = 5,
                maxReceiveCount = 5
            )
        )

        // 1. 큐 생성
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(request)
            .exchange()
            .expectStatus().isCreated
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.queueName").isEqualTo(queueName)
            .jsonPath("$.data.attributes.visibilityTimeoutSeconds").isEqualTo(60)

        // 2. 큐 목록 조회로 생성 확인
        webTestClient.get()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").isArray
            .jsonPath("$.data[?(@.queueName == '$queueName')]").exists()

        // 3. 큐 삭제
        webTestClient.delete()
            .uri("/api/v1/queues/$queueName")
            .header("X-API-Key", "test-api-key")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").value { message: String ->
                assert(message.contains("성공적으로 삭제"))
            }

        // 4. 삭제 후 다시 삭제 시도 (에러 확인)
        webTestClient.delete()
            .uri("/api/v1/queues/$queueName")
            .header("X-API-Key", "test-api-key")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(false)
            .jsonPath("$.error.code").isEqualTo("QUEUE_NOT_FOUND")
    }

    @Test
    fun `페이징과 필터링 테스트`() {
        // 테스트용 큐 3개 생성
        val testQueues = listOf("page-test-1", "page-test-2", "other-queue")
        
        testQueues.forEach { queueName ->
            val request = CreateQueueRequest(queueName = queueName)
            webTestClient.post()
                .uri("/api/v1/queues")
                .header("X-API-Key", "test-api-key")
                .contentType(MediaType.APPLICATION_JSON)
                .bodyValue(request)
                .exchange()
                .expectStatus().isCreated
        }

        // limit=2로 페이징 테스트
        webTestClient.get()
            .uri("/api/v1/queues?limit=2")
            .header("X-API-Key", "test-api-key")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").isArray
            .jsonPath("$.data.length()").isEqualTo(2)

        // 생성한 테스트 큐들 삭제
        testQueues.forEach { queueName ->
            webTestClient.delete()
                .uri("/api/v1/queues/$queueName")
                .header("X-API-Key", "test-api-key")
                .exchange()
                .expectStatus().isOk
        }
    }

    @Test
    fun `잘못된 API Key로 인증 실패 테스트`() {
        webTestClient.get()
            .uri("/api/v1/queues")
            .header("X-API-Key", "invalid-api-key")
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(false)
            .jsonPath("$.error.code").isEqualTo("UNAUTHORIZED")
    }

    @Test
    fun `유효성 검사 실패 테스트`() {
        // 빈 큐 이름으로 테스트 (init 블록에서 IllegalArgumentException 발생 예상)
        try {
            CreateQueueRequest(queueName = "")
            assert(false) { "예외가 발생해야 합니다" }
        } catch (e: IllegalArgumentException) {
            assert(e.message?.contains("비어있을 수 없습니다") == true)
        }

        // 잘못된 큐 이름 패턴 테스트
        try {
            CreateQueueRequest(queueName = "invalid@queue#name")
            assert(false) { "예외가 발생해야 합니다" }
        } catch (e: IllegalArgumentException) {
            assert(e.message?.contains("영숫자") == true)
        }
    }

    @Test
    fun `큐 속성 수정 테스트`() {
        val testQueueName = "update-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )
        
        val createResponse = webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(createResponse!!.success)
        
        // 2. 큐 속성 수정
        val updateRequest = UpdateQueueRequest(
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 60,  // 30 → 60
                messageRetentionPeriod = 604800, // 1209600 → 604800 (7일)
                delaySeconds = 10,               // 0 → 10
                maxReceiveCount = 5              // 3 → 5
            )
        )
        
        val updateResponse = webTestClient.put()
            .uri("/api/v1/queues/$testQueueName/attributes")
            .header("X-API-Key", apiKey)
            .bodyValue(updateRequest)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(updateResponse!!.success)
        
        // 3. 속성 조회로 변경 확인
        val getResponse = webTestClient.get()
            .uri("/api/v1/queues/$testQueueName/attributes")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(getResponse!!.success)
        
        // 4. 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 큐 속성 수정 테스트 통과")
    }
}
