package com.example.sqs.admin.domain

import java.time.Instant

/**
 * 시스템 전체 통계 정보
 */
data class SystemStats(
    val totalTenants: Long,
    val totalQueues: Long,
    val totalMessages: Long,
    val totalMessagesProcessed: Long,
    val systemUptime: Long,
    val memoryUsage: MemoryUsage,
    val redisStats: RedisStats,
    val timestamp: Instant = Instant.now()
)

/**
 * 메모리 사용량 정보
 */
data class MemoryUsage(
    val usedMemory: Long,
    val maxMemory: Long,
    val usagePercentage: Double
)

/**
 * Redis 통계 정보
 */
data class RedisStats(
    val connectedClients: Int,
    val usedMemory: Long,
    val keyCount: Long,
    val opsPerSecond: Double
)

/**
 * 테넌트별 사용량 통계
 */
data class TenantUsageStats(
    val tenantId: String,
    val tenantName: String,
    val queueCount: Long,
    val messageCount: Long,
    val messagesProcessedToday: Long,
    val storageUsed: Long,
    val apiCallsToday: Long,
    val lastActivity: Instant?,
    val isActive: Boolean
)

/**
 * 관리자 대시보드 정보
 */
data class AdminDashboard(
    val systemStats: SystemStats,
    val tenantUsageStats: List<TenantUsageStats>,
    val recentActivities: List<ActivityLog>,
    val systemAlerts: List<SystemAlert>
)

/**
 * 활동 로그
 */
data class ActivityLog(
    val id: String,
    val tenantId: String?,
    val action: String,
    val description: String,
    val timestamp: Instant,
    val severity: ActivitySeverity
)

/**
 * 활동 로그 심각도
 */
enum class ActivitySeverity {
    INFO,
    WARNING,
    ERROR
}

/**
 * 시스템 알림
 */
data class SystemAlert(
    val id: String,
    val type: AlertType,
    val title: String,
    val message: String,
    val severity: AlertSeverity,
    val timestamp: Instant,
    val isResolved: Boolean = false
)

enum class AlertType {
    MEMORY_HIGH, DISK_FULL, QUEUE_OVERFLOW, DLQ_THRESHOLD, TENANT_QUOTA_EXCEEDED
}

enum class AlertSeverity {
    LOW, MEDIUM, HIGH, CRITICAL
}
