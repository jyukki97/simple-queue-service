package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.*
import kotlinx.coroutines.flow.Flow
import java.time.Duration
import java.time.Instant

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
 * 배치 처리 결과
 */
data class BatchResult(
    val successful: Int,
    val failed: Int,
    val errors: List<String> = emptyList()
)

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

/**
 * 계층화 관리 서비스
 */
interface TieringManagerService {
    
    /**
     * 메시지 위치 추적
     */
    suspend fun trackMessageLocation(messageId: String, tier: StorageTier)
    
    /**
     * 메시지 위치 조회
     */
    suspend fun getMessageLocation(messageId: String): MessageLocation?
    
    /**
     * 계층 간 메시지 이관
     */
    suspend fun migrateMessage(messageId: String, fromTier: StorageTier, toTier: StorageTier): Boolean
    
    /**
     * 배치 이관
     */
    suspend fun migrateBatch(
        messages: List<Message>, 
        fromTier: StorageTier, 
        toTier: StorageTier,
        reason: String
    ): BatchResult
    
    /**
     * 자동 계층화 실행
     */
    suspend fun performAutoTiering()
    
    /**
     * 메시지 승격 (하위 계층 → 상위 계층)
     */
    suspend fun promoteMessage(messageId: String, targetTier: StorageTier): Boolean
    
    /**
     * 예측 기반 승격
     */
    suspend fun performPredictivePromotion(queueName: String)
    
    /**
     * 계층화 정책 적용
     */
    suspend fun applyTieringPolicy(policy: TieringPolicy)
    
    /**
     * 이관 이력 조회
     */
    suspend fun getMigrationHistory(messageId: String): List<TierMigrationRecord>
    
    /**
     * 전체 계층 통계
     */
    suspend fun getAllTierMetrics(): Map<StorageTier, TierMetrics>
}

/**
 * 메시지 접근 패턴 분석 서비스
 */
interface AccessPatternAnalyzer {
    
    /**
     * 메시지 접근 기록
     */
    suspend fun recordAccess(messageId: String, accessType: AccessType)
    
    /**
     * 큐 접근 패턴 분석
     */
    suspend fun analyzeQueuePattern(queueName: String): AccessPattern
    
    /**
     * 인기도 점수 계산
     */
    suspend fun calculatePopularityScore(messageId: String): Double
    
    /**
     * 승격 후보 조회
     */
    suspend fun getPromotionCandidates(
        queueName: String, 
        targetTier: StorageTier,
        limit: Int = 100
    ): List<String>
}

/**
 * 접근 타입
 */
enum class AccessType {
    READ, WRITE, DELETE, PEEK
}

/**
 * 접근 패턴
 */
data class AccessPattern(
    val queueName: String,
    val accessFrequency: Double,
    val peakHours: List<Int>,
    val avgMessageLifetime: Duration,
    val readWriteRatio: Double,
    val burstPattern: BurstPattern?
)

/**
 * 버스트 패턴
 */
data class BurstPattern(
    val intensity: Double,
    val duration: Duration,
    val frequency: Duration,
    val predictedNextBurst: Instant?
)
