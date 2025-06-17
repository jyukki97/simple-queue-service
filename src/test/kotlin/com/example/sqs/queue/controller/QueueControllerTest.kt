package com.example.sqs.queue.controller

import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows

class QueueControllerTest {

    @Test
    fun `큐 도메인 모델 생성 테스트`() {
        val request = CreateQueueRequest(
            queueName = "test-queue",
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )
        
        assert(request.queueName == "test-queue")
        assert(request.attributes.visibilityTimeoutSeconds == 30)
        println("✅ 큐 도메인 모델 생성 테스트 통과")
    }

    @Test
    fun `큐 Attributes 유효성 검사 테스트`() {
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            delaySeconds = 0,
            maxReceiveCount = 3
        )
        
        assert(attributes.visibilityTimeoutSeconds in 0..43200)
        assert(attributes.messageRetentionPeriod in 60..1209600)
        assert(attributes.delaySeconds in 0..900)
        assert(attributes.maxReceiveCount in 1..1000)
        println("✅ 큐 Attributes 유효성 검사 테스트 통과")
    }
    
    @Test
    fun `큐 Attributes 범위 초과시 예외 발생`() {
        assertThrows<IllegalArgumentException> {
            QueueAttributes(visibilityTimeoutSeconds = 50000) // 43200 초과
        }
        
        assertThrows<IllegalArgumentException> {
            QueueAttributes(messageRetentionPeriod = 30) // 60 미만
        }
        
        assertThrows<IllegalArgumentException> {
            QueueAttributes(delaySeconds = 1000) // 900 초과
        }
        
        assertThrows<IllegalArgumentException> {
            QueueAttributes(maxReceiveCount = 0) // 1 미만
        }
        
        println("✅ 큐 Attributes 범위 초과 예외 테스트 통과")
    }
}
