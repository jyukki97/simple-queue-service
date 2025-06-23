package com.example.sqs.common.util

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.assertThrows
import java.time.Instant

/**
 * CommonUtils 단위 테스트
 */
class CommonUtilsTest {
    
    @Test
    @DisplayName("UUID 생성 테스트")
    fun testGenerateId() {
        // When
        val id1 = CommonUtils.generateId()
        val id2 = CommonUtils.generateId()
        
        // Then
        assert(id1.isNotEmpty())
        assert(id2.isNotEmpty())
        assert(id1 != id2) // 다른 ID 생성 확인
    }
    
    @Test
    @DisplayName("타임스탬프 포맷팅 테스트")
    fun testFormatTimestamp() {
        // Given
        val timestamp = Instant.parse("2023-01-01T12:00:00Z")
        
        // When
        val formatted = CommonUtils.formatTimestamp(timestamp)
        
        // Then
        assert(formatted == "2023-01-01T12:00:00Z")
    }
    
    @Test
    @DisplayName("타임스탬프 파싱 테스트")
    fun testParseTimestamp() {
        // Given
        val timestampString = "2023-01-01T12:00:00Z"
        
        // When
        val parsed = CommonUtils.parseTimestamp(timestampString)
        
        // Then
        assert(parsed == Instant.parse(timestampString))
    }
    
    @Test
    @DisplayName("바이트 크기 포맷팅 테스트")
    fun testFormatBytes() {
        assert(CommonUtils.formatBytes(0) == "0.00 B")
        assert(CommonUtils.formatBytes(1024) == "1.00 KB")
        assert(CommonUtils.formatBytes(1024 * 1024) == "1.00 MB")
        assert(CommonUtils.formatBytes(1024 * 1024 * 1024) == "1.00 GB")
    }
    
    @Test
    @DisplayName("키 생성 테스트")
    fun testBuildKey() {
        // When
        val key = CommonUtils.buildKey("prefix", "middle", "suffix")
        
        // Then
        assert(key == "prefix:middle:suffix")
    }
    
    @Test
    @DisplayName("안전한 문자열 비교 테스트")
    fun testSafeEquals() {
        assert(CommonUtils.safeEquals("test", "test"))
        assert(!CommonUtils.safeEquals("test", "other"))
        assert(!CommonUtils.safeEquals(null, "test"))
        assert(!CommonUtils.safeEquals("test", null))
        assert(!CommonUtils.safeEquals(null, null))
    }
}

/**
 * ValidationUtils 단위 테스트
 */
class ValidationUtilsTest {
    
    @Test
    @DisplayName("유효한 큐 이름 검증 테스트")
    fun testValidQueueName() {
        // 이러한 큐 이름들은 예외를 발생시키지 않아야 함
        ValidationUtils.validateQueueName("valid-queue")
        ValidationUtils.validateQueueName("valid_queue")
        ValidationUtils.validateQueueName("validQueue123")
    }
    
    @Test
    @DisplayName("잘못된 큐 이름 검증 테스트")
    fun testInvalidQueueName() {
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateQueueName("")
        }
        
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateQueueName("   ")
        }
        
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateQueueName("a".repeat(81)) // 80자 초과
        }
        
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateQueueName("invalid@queue") // 특수문자 포함
        }
    }
    
    @Test
    @DisplayName("유효한 메시지 바디 검증 테스트")
    fun testValidMessageBody() {
        ValidationUtils.validateMessageBody("Valid message", 1000)
    }
    
    @Test
    @DisplayName("잘못된 메시지 바디 검증 테스트")
    fun testInvalidMessageBody() {
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateMessageBody("", 1000)
        }
        
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateMessageBody("   ", 1000)
        }
        
        assertThrows<IllegalArgumentException> {
            ValidationUtils.validateMessageBody("Too large message", 5) // 크기 초과
        }
    }
}

/**
 * MetricsUtils 단위 테스트
 */
class MetricsUtilsTest {
    
    @Test
    @DisplayName("메트릭 이름 생성 테스트")
    fun testBuildMetricName() {
        val metricName = MetricsUtils.buildMetricName("sqs", "messages.sent")
        assert(metricName == "sqs.messages.sent")
    }
    
    @Test
    @DisplayName("태그 문자열 변환 테스트")
    fun testTagsToString() {
        val tags = mapOf("queue" to "test", "tenant" to "demo")
        val result = MetricsUtils.tagsToString(tags)
        
        // 순서는 보장되지 않으므로 포함 여부만 확인
        assert(result.contains("queue=test"))
        assert(result.contains("tenant=demo"))
        assert(result.contains(","))
    }
    
    @Test
    @DisplayName("처리율 계산 테스트")
    fun testCalculateRate() {
        assert(MetricsUtils.calculateRate(100, 10) == 10.0)
        assert(MetricsUtils.calculateRate(0, 10) == 0.0)
        assert(MetricsUtils.calculateRate(100, 0) == 0.0)
    }
}
