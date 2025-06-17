package com.example.sqs.metrics.domain

import java.time.Instant

/**
 * 큐별 메트릭 정보
 */
data class QueueMetrics(
    val queueName: String,
    val tenantId: String,
    val timestamp: Instant,
    
    // 메시지 수 메트릭
    val messagesAvailable: Long,
    val messagesInFlight: Long,
    val messagesDelayed: Long,
    val messagesInDlq: Long,
    
    // 처리량 메트릭
    val messagesSentPerMinute: Double,
    val messagesReceivedPerMinute: Double,
    val messagesDeletedPerMinute: Double,
    val messagesFailedPerMinute: Double,
    
    // 성능 메트릭
    val averageAgeOfOldestMessage: Long,
    val averageProcessingTime: Double,
    val errorRate: Double,
    
    // 용량 메트릭
    val totalMessageSize: Long,
    val averageMessageSize: Double
)

/**
 * 시스템 전체 메트릭 정보
 */
data class SystemMetrics(
    val timestamp: Instant,
    
    // 전체 통계
    val totalQueues: Long,
    val totalMessages: Long,
    val totalTenants: Long,
    
    // 성능 통계
    val totalThroughputPerSecond: Double,
    val averageLatency: Double,
    val errorRate: Double,
    
    // 리소스 사용량
    val memoryUsageBytes: Long,
    val cpuUsagePercent: Double,
    val redisConnectionsActive: Int,
    val redisConnectionsIdle: Int
)

/**
 * 메트릭 시계열 데이터
 */
data class MetricTimeSeries(
    val metricName: String,
    val labels: Map<String, String>,
    val dataPoints: List<MetricDataPoint>
)

/**
 * 메트릭 데이터 포인트
 */
data class MetricDataPoint(
    val timestamp: Instant,
    val value: Double
)

/**
 * 알림 규칙
 */
data class AlertRule(
    val id: String,
    val name: String,
    val description: String,
    val metricName: String,
    val condition: AlertCondition,
    val threshold: Double,
    val duration: Long, // 초 단위
    val isEnabled: Boolean,
    val createdAt: Instant
)

/**
 * 알림 조건
 */
enum class AlertCondition {
    GREATER_THAN,
    LESS_THAN,
    EQUAL_TO,
    NOT_EQUAL_TO
}

/**
 * 알림 이벤트
 */
data class AlertEvent(
    val id: String,
    val ruleId: String,
    val ruleName: String,
    val message: String,
    val severity: AlertSeverity,
    val timestamp: Instant,
    val metricValue: Double,
    val threshold: Double,
    val queueName: String? = null,
    val tenantId: String? = null
)

/**
 * 알림 심각도
 */
enum class AlertSeverity {
    INFO,
    WARNING,
    ERROR,
    CRITICAL
}
