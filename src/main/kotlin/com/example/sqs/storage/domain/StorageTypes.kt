package com.example.sqs.storage.domain

import java.time.Instant

/**
 * 스토리지 계층 구분
 */
enum class StorageTier {
    HOT,    // Redis 메모리 - 즉시 처리 메시지
    WARM,   // Redis 디스크 - 중간 빈도 접근
    COLD    // 외부 스토리지 - 장기 보관
}

/**
 * 메시지 위치 정보
 */
data class MessageLocation(
    val tier: StorageTier,
    val lastMovedAt: Instant,
    val createdAt: Instant
)

/**
 * 계층 이관 기록
 */
data class TierMigrationRecord(
    val messageId: String,
    val fromTier: StorageTier,
    val toTier: StorageTier,
    val timestamp: Instant,
    val reason: String,
    val size: Long
)

/**
 * 스토리지 계층별 메트릭
 */
data class TierMetrics(
    val tier: StorageTier,
    val messageCount: Long,
    val storageUsageBytes: Long,
    val avgAccessTimeMs: Double,
    val hitRate: Double,
    val migrationRate: Double,
    val errorRate: Double,
    val lastUpdated: Instant
)

/**
 * 승격 힌트 (사용자 제공)
 */
data class PromotionHint(
    val queueName: String,
    val hintType: PromotionHintType,
    val timeWindowSeconds: Long,
    val expectedThroughput: Int? = null,
    val priority: Int = 0
)

enum class PromotionHintType {
    HIGH_PRIORITY_PROCESSING,
    BATCH_PROCESSING_SCHEDULED,
    EXPECTED_BURST_TRAFFIC,
    MANUAL_PROMOTION
}
