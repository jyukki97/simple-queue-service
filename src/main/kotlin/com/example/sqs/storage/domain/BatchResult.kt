package com.example.sqs.storage.domain

/**
 * 배치 처리 결과를 나타내는 공통 데이터 클래스
 */
data class BatchResult(
    val successCount: Int,
    val failCount: Int,
    val errors: List<String>
)

/**
 * 배치 이동 결과
 */
data class BatchMoveResult(
    val totalRequested: Int,
    val successCount: Int,
    val failCount: Int,
    val errors: List<String>
)

/**
 * WARM 스토리지 통계
 */
data class WarmStorageStats(
    val totalMessages: Int,
    val activeMessages: Int,
    val expiredMessages: Int,
    val estimatedSizeBytes: Long
)
