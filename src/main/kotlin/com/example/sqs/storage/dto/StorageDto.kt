package com.example.sqs.storage.dto

import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierStats
import com.example.sqs.storage.domain.PromotionHintType

/**
 * 메시지 위치 응답 DTO
 */
data class MessageLocationResponse(
    val messageId: String,
    val currentTier: String,
    val lastMovedAt: String,
    val createdAt: String
)

/**
 * 스토리지 통계 응답 DTO
 */
data class StorageStatsResponse(
    val totalQueues: Int,
    val queueStats: List<QueueStorageStats>,
    val aggregatedStats: AggregatedStats
)

/**
 * 큐별 스토리지 통계 DTO
 */
data class QueueStorageStats(
    val queueName: String,
    val queueType: String,
    val hotStats: TierStats,
    val warmStats: TierStats,
    val coldStats: TierStats
)

/**
 * 집계된 통계 DTO
 */
data class AggregatedStats(
    val totalMessages: Long,
    val totalHotMessages: Long,
    val totalWarmMessages: Long,
    val totalColdMessages: Long,
    val totalStorageBytes: Long
)

/**
 * 메시지 승격 요청 DTO
 */
data class PromoteMessageRequest(
    val targetTier: String,
    val reason: String = ""
)

/**
 * 메시지 승격 응답 DTO
 */
data class PromoteMessageResponse(
    val messageId: String,
    val fromTier: String,
    val toTier: String,
    val success: Boolean,
    val reason: String
)

/**
 * 승격 결과 DTO
 */
data class PromoteResult(
    val fromTier: String,
    val toTier: String,
    val success: Boolean
)

/**
 * 승격 요청 DTO (관리자용)
 */
data class PromotionRequest(
    val targetTier: StorageTier,
    val reason: String? = null
)

/**
 * 배치 이관 요청 DTO
 */
data class BatchMigrationRequest(
    val messageIds: List<String>,
    val fromTier: StorageTier,
    val toTier: StorageTier,
    val reason: String
)

/**
 * 승격 힌트 요청 DTO
 */
data class PromotionHintRequest(
    val hintType: PromotionHintType,
    val timeWindowSeconds: Long,
    val expectedThroughput: Int? = null,
    val priority: Int = 0
)

/**
 * 스토리지 계층별 메시지 응답 DTO
 */
data class TierMessagesResponse(
    val queueName: String,
    val tier: String,
    val messages: List<TierMessageInfo>,
    val totalCount: Int,
    val offset: Int,
    val maxMessages: Int
)

/**
 * 스토리지 계층별 메시지 요약 응답 DTO
 */
data class StorageMessagesSummaryResponse(
    val queueName: String,
    val hotMessages: TierMessagesSummary,
    val warmMessages: TierMessagesSummary,
    val coldMessages: TierMessagesSummary
)

/**
 * 스토리지 계층 메시지 요약 DTO
 */
data class TierMessagesSummary(
    val tier: String,
    val messageCount: Long,
    val sampleMessages: List<TierMessageInfo>,
    val totalSizeBytes: Long
)

/**
 * 스토리지 계층의 메시지 정보 DTO
 */
data class TierMessageInfo(
    val messageId: String,
    val messageBody: String,
    val attributes: Map<String, String>,
    val sentTimestamp: String,
    val receivedCount: Int,
    val tier: String,
    val movedToTierAt: String?,
    val sizeBytes: Long,
    val compressionType: String?
)
