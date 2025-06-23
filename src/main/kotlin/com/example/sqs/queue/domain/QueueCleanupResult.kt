package com.example.sqs.queue.domain

/**
 * 큐 정리 작업 결과
 */
data class QueueCleanupResult(
    val queueName: String,
    val deletedRedisKeys: Long = 0,
    val deletedColdFiles: Int = 0,
    val success: Boolean = true,
    val error: String? = null,
    val statistics: CleanupStatistics? = null,
    val summary: String? = null
)

/**
 * 정리 작업 통계
 */
data class CleanupStatistics(
    val totalKeysScanned: Long = 0,
    val totalFilesScanned: Int = 0,
    val processingTimeMs: Long = 0
)
