package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import java.time.Duration

/**
 * 스토리지 계층 인터페이스
 */
interface StorageTierService {
    val tier: StorageTier

    /**
     * 메시지 저장
     */
    suspend fun saveMessage(message: Message): Boolean

    /**
     * 메시지 일괄 저장
     */
    suspend fun saveBatch(messages: List<Message>): BatchResult

    /**
     * 메시지 조회
     */
    suspend fun getMessage(messageId: String): Message?

    /**
     * 큐의 가장 오래된 메시지 조회 (FIFO용)
     */
    suspend fun getOldestMessage(queueName: String): Message?

    /**
     * 큐에서 메시지 조회 (Standard용)
     */
    suspend fun getMessages(queueName: String, maxCount: Int): List<Message>

    /**
     * 메시지 삭제
     */
    suspend fun deleteMessage(messageId: String): Boolean

    /**
     * 메시지 일괄 삭제
     */
    suspend fun deleteBatch(messageIds: List<String>): BatchResult

    /**
     * 만료된 메시지 조회
     */
    suspend fun getExpiredMessages(maxAge: Duration, limit: Int = 1000): List<Message>

    /**
     * 계층 통계 조회
     */
    suspend fun getMetrics(): TierMetrics

    /**
     * 스토리지 사용량 조회
     */
    suspend fun getUsageInfo(): StorageUsageInfo
}

/**
 * 스토리지 사용량 정보
 */
data class StorageUsageInfo(
    val tier: StorageTier,
    val totalCapacityBytes: Long,
    val usedBytes: Long,
    val availableBytes: Long,
    val usagePercent: Double,
    val messageCount: Long
)
