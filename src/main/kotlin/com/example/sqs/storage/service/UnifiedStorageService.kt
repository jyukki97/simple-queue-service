package com.example.sqs.storage.service

import com.example.sqs.storage.domain.PromotionHint
import com.example.sqs.storage.domain.PromotionHintType
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.impl.ColdStorageService
import com.example.sqs.storage.service.impl.HotStorageService
import com.example.sqs.storage.service.impl.WarmStorageService
import org.springframework.stereotype.Service

/**
 * 통합 스토리지 서비스
 * 핫/웜/콜드 스토리지를 통합하여 메시지 CRUD 작업을 처리
 */
@Service
class UnifiedStorageService(
    private val hotStorage: HotStorageService,
    private val warmStorage: WarmStorageService,
    private val coldStorage: ColdStorageService,
    private val tieringManager: TieringManagerService
) {

    /**
     * 메시지 삭제
     */
    suspend fun deleteMessage(messageId: String): Boolean {
        val location = tieringManager.getMessageLocation(messageId)
        if (location != null) {
            val storageService = getStorageService(location.tier)
            return storageService.deleteMessage(messageId)
        }

        // 위치 정보가 없으면 모든 계층에서 삭제 시도
        return hotStorage.deleteMessage(messageId) ||
                warmStorage.deleteMessage(messageId) ||
                coldStorage.deleteMessage(messageId)
    }

    /**
     * 스토리지 통계 조회
     */
    suspend fun getStorageStats(): Map<StorageTier, TierMetrics> {
        return tieringManager.getAllTierMetrics()
    }

    /**
     * 사용자 힌트 기반 승격
     */
    suspend fun applyPromotionHint(hint: PromotionHint) {
        when (hint.hintType) {
            PromotionHintType.HIGH_PRIORITY_PROCESSING -> {
                // 해당 큐의 최근 메시지들을 핫으로 승격
                tieringManager.performPredictivePromotion(hint.queueName)
            }

            PromotionHintType.BATCH_PROCESSING_SCHEDULED -> {
                // 배치 처리 예정이므로 많은 메시지를 웜으로 승격
                // 구현 로직
            }

            PromotionHintType.EXPECTED_BURST_TRAFFIC -> {
                // 버스트 트래픽 예상이므로 사전 승격
                // 구현 로직
            }

            PromotionHintType.MANUAL_PROMOTION -> {
                // 수동 승격 처리
                // 구현 로직
            }
        }
    }

    private fun getStorageService(tier: StorageTier): StorageTierService {
        return when (tier) {
            StorageTier.HOT -> hotStorage
            StorageTier.WARM -> warmStorage
            StorageTier.COLD -> coldStorage
        }
    }
}
