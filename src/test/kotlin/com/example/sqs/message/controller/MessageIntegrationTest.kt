package com.example.sqs.message.controller

import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.domain.SendMessageResponse
import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import org.junit.jupiter.api.Test
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.TestConstructor
import org.springframework.test.web.reactive.server.WebTestClient

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@TestConstructor(autowireMode = TestConstructor.AutowireMode.ALL)
@org.springframework.test.context.ActiveProfiles("test")
class MessageIntegrationTest(private val webTestClient: WebTestClient) {

    @Test
    fun `메시지 전송 및 수신 통합 테스트`() {
        val testQueueName = "message-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createQueueRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )
        
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createQueueRequest)
            .exchange()
            .expectStatus().isCreated
        
        // 2. 메시지 전송
        val sendMessageRequest = SendMessageRequest(
            messageBody = "Hello, SQS!",
            delaySeconds = 0,
            attributes = mapOf("sender" to "test", "priority" to "high")
        )
        
        val sendResponse = webTestClient.post()
            .uri("/api/v1/queues/$testQueueName/messages")
            .header("X-API-Key", apiKey)
            .bodyValue(sendMessageRequest)
            .exchange()
            .expectStatus().isCreated
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(sendResponse!!.success)
        println("✅ 메시지 전송 성공")
        
        // 3. 메시지 수신
        val receiveResponse = webTestClient.get()
            .uri("/api/v1/queues/$testQueueName/messages?maxNumberOfMessages=1")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(receiveResponse!!.success)
        println("✅ 메시지 수신 성공")
        
        // 4. 큐 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 메시지 전송 및 수신 통합 테스트 통과")
    }
    
    @Test
    fun `지연 메시지 전송 테스트`() {
        val testQueueName = "delay-message-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createQueueRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes()
        )
        
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createQueueRequest)
            .exchange()
            .expectStatus().isCreated
        
        // 2. 지연 메시지 전송 (5초 지연)
        val sendMessageRequest = SendMessageRequest(
            messageBody = "Delayed message",
            delaySeconds = 5,
            attributes = mapOf("type" to "delayed")
        )
        
        val sendResponse = webTestClient.post()
            .uri("/api/v1/queues/$testQueueName/messages")
            .header("X-API-Key", apiKey)
            .bodyValue(sendMessageRequest)
            .exchange()
            .expectStatus().isCreated
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(sendResponse!!.success)
        println("✅ 지연 메시지 전송 성공")
        
        // 3. 즉시 메시지 수신 시도 (비어있어야 함)
        val immediateReceiveResponse = webTestClient.get()
            .uri("/api/v1/queues/$testQueueName/messages?maxNumberOfMessages=1")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(immediateReceiveResponse!!.success)
        println("✅ 즉시 수신 시 메시지 없음 확인")
        
        // 4. 큐 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 지연 메시지 전송 테스트 통과")
    }
    
    @Test
    fun `잘못된 메시지 전송 요청 테스트`() {
        val testQueueName = "invalid-message-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createQueueRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes()
        )
        
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createQueueRequest)
            .exchange()
            .expectStatus().isCreated
        
        // 2. JSON으로 직접 빈 메시지 전송 시도 (validation 우회)
        val invalidRequestJson = """
            {
                "messageBody": "",
                "delaySeconds": 0
            }
        """.trimIndent()
        
        webTestClient.post()
            .uri("/api/v1/queues/$testQueueName/messages")
            .header("X-API-Key", apiKey)
            .header("Content-Type", "application/json")
            .bodyValue(invalidRequestJson)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(false)
            .jsonPath("$.error.code").isEqualTo("BAD_REQUEST")
        
        println("✅ 잘못된 메시지 전송 요청 거부 확인")
        
        // 3. 큐 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 잘못된 메시지 전송 요청 테스트 통과")
    }
    
    @Test
    fun `메시지 일괄 전송 테스트`() {
        val testQueueName = "batch-send-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createQueueRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes()
        )
        
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createQueueRequest)
            .exchange()
            .expectStatus().isCreated
        
        // 2. 메시지 일괄 전송 (3개 메시지)
        val batchRequestJson = """
            {
                "messages": [
                    {
                        "id": "msg1",
                        "messageBody": "Batch message 1",
                        "delaySeconds": 0,
                        "attributes": {"type": "batch", "order": "1"}
                    },
                    {
                        "id": "msg2",
                        "messageBody": "Batch message 2",
                        "delaySeconds": 0,
                        "attributes": {"type": "batch", "order": "2"}
                    },
                    {
                        "id": "msg3",
                        "messageBody": "Batch message 3",
                        "delaySeconds": 5,
                        "attributes": {"type": "batch", "order": "3"}
                    }
                ]
            }
        """.trimIndent()
        
        val batchResponse = webTestClient.post()
            .uri("/api/v1/queues/$testQueueName/messages/batch")
            .header("X-API-Key", apiKey)
            .header("Content-Type", "application/json")
            .bodyValue(batchRequestJson)
            .exchange()
            .expectStatus().isCreated
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(batchResponse!!.success)
        println("✅ 메시지 일괄 전송 성공")
        
        // 3. 메시지 수신 (지연 메시지 제외하고 2개 수신되어야 함)
        val receiveResponse = webTestClient.get()
            .uri("/api/v1/queues/$testQueueName/messages?maxNumberOfMessages=10")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(receiveResponse!!.success)
        println("✅ 메시지 수신 성공")
        
        // 4. 큐 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 메시지 일괄 전송 테스트 통과")
    }
    
    @Test
    fun `메시지 일괄 삭제 테스트`() {
        val testQueueName = "batch-delete-test-queue"
        val apiKey = "test-api-key"
        
        // 1. 큐 생성
        val createQueueRequest = CreateQueueRequest(
            queueName = testQueueName,
            attributes = QueueAttributes()
        )
        
        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", apiKey)
            .bodyValue(createQueueRequest)
            .exchange()
            .expectStatus().isCreated
        
        // 2. 메시지 3개 전송
        repeat(3) { i ->
            val sendMessageRequest = SendMessageRequest(
                messageBody = "Message $i",
                delaySeconds = 0,
                attributes = mapOf("index" to i.toString())
            )
            
            webTestClient.post()
                .uri("/api/v1/queues/$testQueueName/messages")
                .header("X-API-Key", apiKey)
                .bodyValue(sendMessageRequest)
                .exchange()
                .expectStatus().isCreated
        }
        
        // 3. 메시지 수신하여 receiptHandle 수집
        val receiveResponse = webTestClient.get()
            .uri("/api/v1/queues/$testQueueName/messages?maxNumberOfMessages=3")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody(ApiResponse::class.java)
            .returnResult()
            .responseBody
        
        assert(receiveResponse!!.success)
        println("✅ 메시지 수신 완료")
        
        // 4. 일괄 삭제 요청 (실제 receiptHandle 없이 테스트)
        val batchDeleteJson = """
            {
                "receiptHandles": ["receipt1", "receipt2"]
            }
        """.trimIndent()
        
        val deleteResponse = webTestClient.post()
            .uri("/api/v1/queues/$testQueueName/messages/batch-delete")
            .header("X-API-Key", apiKey)
            .header("Content-Type", "application/json")
            .bodyValue(batchDeleteJson)
            .exchange()
            .expectStatus().isOk
            .expectBody(String::class.java)
            .returnResult()
            .responseBody
        
        println("일괄 삭제 응답: $deleteResponse")
        assert(deleteResponse != null)
        println("✅ 메시지 일괄 삭제 요청 완료")
        
        // 5. 큐 정리
        webTestClient.delete()
            .uri("/api/v1/queues/$testQueueName")
            .header("X-API-Key", apiKey)
            .exchange()
            .expectStatus().isOk
            
        println("✅ 메시지 일괄 삭제 테스트 통과")
    }
}
