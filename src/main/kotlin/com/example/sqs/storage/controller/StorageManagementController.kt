package com.example.sqs.storage.controller

import com.example.sqs.common.ApiResponse
import com.example.sqs.storage.domain.*
import com.example.sqs.storage.service.*
import org.springframework.web.bind.annotation.*

/**
 * 스토리지 계층화 관리 API
 */
@RestController
@RequestMapping("/api/v1/storage")
class StorageManagementController(
    private val tieringManager: TieringManagerService,
    private val unifiedStorage: UnifiedStorageService
) {
    
    /**
     * 전체 스토리지 통계 조회
     */
    @GetMapping("/stats")
    suspend fun getStorageStats(): ApiResponse<Map<StorageTier, TierMetrics>> {
        val stats = unifiedStorage.getStorageStats()
        return ApiResponse.success(stats)
    }
    
    /**
     * 특정 계층 메트릭 조회
     */
    @GetMapping("/tiers/{tier}/metrics")
    suspend fun getTierMetrics(@PathVariable tier: StorageTier): ApiResponse<TierMetrics> {
        val allMetrics = tieringManager.getAllTierMetrics()
        val tierMetrics = allMetrics[tier]
        
        return if (tierMetrics != null) {
            ApiResponse.success(tierMetrics)
        } else {
            ApiResponse.error("TIER_NOT_FOUND", "Storage tier not found: $tier")
        }
    }
    
    /**
     * 메시지 위치 정보 조회
     */
    @GetMapping("/messages/{messageId}/location")
    suspend fun getMessageLocation(@PathVariable messageId: String): ApiResponse<MessageLocation?> {
        val location = tieringManager.getMessageLocation(messageId)
        return ApiResponse.success(location)
    }
    
    /**
     * 메시지 이관 이력 조회
     */
    @GetMapping("/messages/{messageId}/migration-history")
    suspend fun getMigrationHistory(@PathVariable messageId: String): ApiResponse<List<TierMigrationRecord>> {
        val history = tieringManager.getMigrationHistory(messageId)
        return ApiResponse.success(history)
    }
    
    /**
     * 수동 메시지 승격
     */
    @PostMapping("/messages/{messageId}/promote")
    suspend fun promoteMessage(
        @PathVariable messageId: String,
        @RequestBody request: PromotionRequest
    ): ApiResponse<Boolean> {
        val result = tieringManager.promoteMessage(messageId, request.targetTier)
        return if (result) {
            ApiResponse.success(true)
        } else {
            ApiResponse.error("PROMOTION_FAILED", "Failed to promote message")
        }
    }
    
    /**
     * 수동 배치 이관
     */
    @PostMapping("/migrate")
    suspend fun migrateBatch(@RequestBody request: BatchMigrationRequest): ApiResponse<BatchResult> {
        // 실제 구현에서는 메시지 조회 후 이관 처리
        // 여기서는 간단한 예시
        return ApiResponse.error("NOT_IMPLEMENTED", "Batch migration not yet implemented")
    }
    
    /**
     * 자동 계층화 정책 조회
     */
    @GetMapping("/policy")
    suspend fun getTieringPolicy(): ApiResponse<TieringPolicy> {
        // 현재 정책 조회 (TieringManagerService에 추가 필요)
        return ApiResponse.error("NOT_IMPLEMENTED", "Policy retrieval not yet implemented")
    }
    
    /**
     * 자동 계층화 정책 설정
     */
    @PostMapping("/policy")
    suspend fun setTieringPolicy(@RequestBody policy: TieringPolicy): ApiResponse<String> {
        tieringManager.applyTieringPolicy(policy)
        return ApiResponse.success("Policy applied successfully")
    }
    
    /**
     * 승격 힌트 제공
     */
    @PostMapping("/queues/{queueName}/promotion-hint")
    suspend fun providePromotionHint(
        @PathVariable queueName: String,
        @RequestBody hint: PromotionHintRequest
    ): ApiResponse<String> {
        val promotionHint = PromotionHint(
            queueName = queueName,
            hintType = hint.hintType,
            timeWindowSeconds = hint.timeWindowSeconds,
            expectedThroughput = hint.expectedThroughput,
            priority = hint.priority
        )
        
        unifiedStorage.applyPromotionHint(promotionHint)
        return ApiResponse.success("Promotion hint applied")
    }
    
    /**
     * 수동 계층화 트리거
     */
    @PostMapping("/trigger-auto-tiering")
    suspend fun triggerAutoTiering(): ApiResponse<String> {
        tieringManager.performAutoTiering()
        return ApiResponse.success("Auto-tiering triggered")
    }
    
    /**
     * 예측 승격 실행
     */
    @PostMapping("/queues/{queueName}/predictive-promotion")
    suspend fun triggerPredictivePromotion(@PathVariable queueName: String): ApiResponse<String> {
        tieringManager.performPredictivePromotion(queueName)
        return ApiResponse.success("Predictive promotion triggered")
    }
}

/**
 * 승격 요청 DTO
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
