package com.example.sqs.storage.domain

import java.time.Instant

/**
 * 스토리지 계층별 통계 정보
 */
data class TierStats(
    val tier: String,
    val messageCount: Long,
    val oldestMessageTime: Instant?,
    val newestMessageTime: Instant?,
    val totalSizeBytes: Long
) {
    /**
     * 개수 기반 초과 여부 확인
     */
    fun isCountExceeded(threshold: Long): Boolean = messageCount > threshold

    /**
     * 시간 기반 초과 여부 확인
     */
    fun isTimeExceeded(thresholdHours: Long): Boolean {
        return oldestMessageTime?.let { oldest ->
            val thresholdTime = Instant.now().minusSeconds(thresholdHours * 3600)
            oldest.isBefore(thresholdTime)
        } ?: false
    }
}
