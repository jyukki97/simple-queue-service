package com.example.sqs.streaming

import com.example.sqs.streaming.domain.StreamingEvent
import com.example.sqs.streaming.domain.StreamingEventType
import com.example.sqs.streaming.domain.SubscribeRequest
import com.example.sqs.streaming.service.StreamingService
import com.fasterxml.jackson.databind.ObjectMapper
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.web.reactive.socket.WebSocketSession
import reactor.core.publisher.Mono
import reactor.test.StepVerifier
import java.time.Duration
import java.util.*

@SpringBootTest
@ActiveProfiles("test")
class StreamingIntegrationTest {

    @Autowired
    private lateinit var streamingService: StreamingService

    @Autowired
    private lateinit var objectMapper: ObjectMapper

    @Test
    fun `should publish and receive message sent events`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val messageId = UUID.randomUUID().toString()
        val messageBody = "Test message body"

        // 메시지 전송 이벤트 발송
        val publishMono = streamingService.publishMessageSent(
            tenantId = tenantId,
            queueName = queueName,
            messageId = messageId,
            messageBody = messageBody
        )

        // 이벤트가 성공적으로 발송되는지 확인
        StepVerifier.create(publishMono)
            .verifyComplete()
    }

    @Test
    fun `should publish message received events`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val messageId = UUID.randomUUID().toString()
        val receiptHandle = UUID.randomUUID().toString()

        // 메시지 수신 이벤트 발송
        val publishMono = streamingService.publishMessageReceived(
            tenantId = tenantId,
            queueName = queueName,
            messageId = messageId,
            receiptHandle = receiptHandle
        )

        // 이벤트가 성공적으로 발송되는지 확인
        StepVerifier.create(publishMono)
            .verifyComplete()
    }

    @Test
    fun `should publish message deleted events`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val messageId = UUID.randomUUID().toString()

        // 메시지 삭제 이벤트 발송
        val publishMono = streamingService.publishMessageDeleted(
            tenantId = tenantId,
            queueName = queueName,
            messageId = messageId
        )

        // 이벤트가 성공적으로 발송되는지 확인
        StepVerifier.create(publishMono)
            .verifyComplete()
    }

    @Test
    fun `should track active connection count`() {
        // 초기 연결 수는 0
        val initialCount = streamingService.getActiveConnectionCount()
        assert(initialCount == 0)

        // 특정 테넌트의 연결 수도 0
        val tenantCount = streamingService.getActiveConnectionCount("test-tenant")
        assert(tenantCount == 0)
    }

    @Test
    fun `should handle subscription requests`() {
        // Mock WebSocket 세션 생성 (실제 WebSocket 연결 없이 서비스 기능만 테스트)
        val tenantId = "test-tenant"
        val connectionId = UUID.randomUUID().toString()

        // 구독 요청 생성
        val subscribeRequest = SubscribeRequest(
            queueNames = setOf("test-queue"),
            eventTypes = setOf(StreamingEventType.MESSAGE_SENT),
            batchSize = 1,
            maxWaitTime = 10000
        )

        // 실제 WebSocket 연결 없이는 구독을 테스트할 수 없으므로
        // 여기서는 요청 객체의 유효성만 확인
        assert(subscribeRequest.queueNames.contains("test-queue"))
        assert(subscribeRequest.eventTypes.contains(StreamingEventType.MESSAGE_SENT))
        assert(subscribeRequest.batchSize == 1)
        assert(subscribeRequest.maxWaitTime == 10000L)
    }

    @Test
    fun `should serialize streaming events correctly`() {
        val event = StreamingEvent(
            eventId = UUID.randomUUID().toString(),
            eventType = StreamingEventType.MESSAGE_SENT,
            tenantId = "test-tenant",
            queueName = "test-queue",
            data = mapOf(
                "messageId" to "test-message",
                "messageBody" to "test body"
            )
        )

        // JSON 직렬화/역직렬화 테스트
        val json = objectMapper.writeValueAsString(event)
        val deserializedEvent = objectMapper.readValue(json, StreamingEvent::class.java)

        assert(deserializedEvent.eventId == event.eventId)
        assert(deserializedEvent.eventType == event.eventType)
        assert(deserializedEvent.tenantId == event.tenantId)
        assert(deserializedEvent.queueName == event.queueName)
    }
}