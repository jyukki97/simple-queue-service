package com.example.sqs.common.util

import java.time.Instant
import java.time.format.DateTimeFormatter
import java.util.*

/**
 * 공통 유틸리티 함수들
 */
object CommonUtils {
    
    private val ISO_FORMATTER = DateTimeFormatter.ISO_INSTANT
    
    /**
     * UUID 생성
     */
    fun generateId(): String = UUID.randomUUID().toString()
    
    /**
     * 타임스탬프를 ISO 형식 문자열로 변환
     */
    fun formatTimestamp(instant: Instant): String = ISO_FORMATTER.format(instant)
    
    /**
     * ISO 형식 문자열을 타임스탬프로 변환
     */
    fun parseTimestamp(timestamp: String): Instant = Instant.parse(timestamp)
    
    /**
     * 현재 시간 반환
     */
    fun now(): Instant = Instant.now()
    
    /**
     * 바이트 크기를 사람이 읽기 쉬운 형태로 변환
     */
    fun formatBytes(bytes: Long): String {
        val units = arrayOf("B", "KB", "MB", "GB", "TB")
        var value = bytes.toDouble()
        var unitIndex = 0
        
        while (value >= 1024.0 && unitIndex < units.size - 1) {
            value /= 1024.0
            unitIndex++
        }
        
        return String.format("%.2f %s", value, units[unitIndex])
    }
    
    /**
     * 키 생성 헬퍼
     */
    fun buildKey(vararg parts: String): String = parts.joinToString(":")
    
    /**
     * 안전한 문자열 비교
     */
    fun safeEquals(str1: String?, str2: String?): Boolean {
        return str1 != null && str2 != null && str1 == str2
    }
}

/**
 * 검증 유틸리티
 */
object ValidationUtils {
    
    /**
     * 큐 이름 검증
     */
    fun validateQueueName(queueName: String) {
        require(queueName.isNotBlank()) { "Queue name cannot be blank" }
        require(queueName.length <= 80) { "Queue name must be 80 characters or less" }
        require(queueName.matches(Regex("^[a-zA-Z0-9_-]+$"))) { 
            "Queue name can only contain alphanumeric characters, hyphens, and underscores" 
        }
    }
    
    /**
     * 메시지 내용 검증
     */
    fun validateMessageBody(body: String, maxSize: Int) {
        require(body.isNotBlank()) { "Message body cannot be blank" }
        require(body.toByteArray().size <= maxSize) { 
            "Message body exceeds maximum size of $maxSize bytes" 
        }
    }
    
    /**
     * 테넌트 ID 검증
     */
    fun validateTenantId(tenantId: String) {
        require(tenantId.isNotBlank()) { "Tenant ID cannot be blank" }
        require(tenantId.matches(Regex("^[a-zA-Z0-9_-]+$"))) { 
            "Tenant ID can only contain alphanumeric characters, hyphens, and underscores" 
        }
    }
}

/**
 * 메트릭 유틸리티
 */
object MetricsUtils {
    
    /**
     * 메트릭 이름 생성
     */
    fun buildMetricName(prefix: String, name: String): String = "${prefix}.${name}"
    
    /**
     * 태그 맵을 문자열로 변환
     */
    fun tagsToString(tags: Map<String, String>): String = 
        tags.entries.joinToString(",") { "${it.key}=${it.value}" }
    
    /**
     * 처리율 계산 (초당)
     */
    fun calculateRate(count: Long, durationSeconds: Long): Double {
        return if (durationSeconds > 0) count.toDouble() / durationSeconds else 0.0
    }
}
