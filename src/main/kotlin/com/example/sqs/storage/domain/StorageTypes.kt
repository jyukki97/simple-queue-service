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
 * 계층화 정책 타입
 */
enum class TieringPolicyType {
    TIME_BASED,     // 시간 기반
    CAPACITY_BASED, // 용량 기반
    ACCESS_PATTERN  // 접근 패턴 기반
}

/**
 * 큐 타입별 처리 전략
 */
enum class QueueType {
    FIFO,     // 순서 보장 (모든 계층 검색)
    STANDARD  // 최적 성능 (핫 우선)
}

/**
 * 메시지 위치 정보
 */
data class MessageLocation(
    val messageId: String,
    val tier: StorageTier,
    val lastAccessTime: Instant,
    val accessCount: Long = 0,
    val migrationHistory: List<TierMigrationRecord> = emptyList()
)

/**
 * 계층 이관 기록
 */
data class TierMigrationRecord(
    val fromTier: StorageTier,
    val toTier: StorageTier,
    val migratedAt: Instant,
    val reason: String,
    val batchId: String? = null
)

/**
 * 계층화 정책 설정
 */
data class TieringPolicy(
    val type: TieringPolicyType,
    val hotToWarmThreshold: TieringThreshold,
    val warmToColdThreshold: TieringThreshold,
    val checkIntervalSeconds: Long = 300, // 5분
    val batchSize: Int = 1000,
    val enabled: Boolean = true
)

/**
 * 계층 이관 임계값
 */
data class TieringThreshold(
    val maxAgeSeconds: Long? = null,
    val maxCapacityPercent: Int? = null,
    val minAccessCount: Long? = null,
    val maxMemoryUsageMB: Long? = null
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
