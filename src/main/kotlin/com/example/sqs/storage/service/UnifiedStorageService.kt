package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.*
import com.example.sqs.storage.service.impl.*
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
     * 메시지 저장 (항상 핫 스토리지에 먼저 저장)
     */
    suspend fun saveMessage(message: Message): Boolean {
        val saved = hotStorage.saveMessage(message)
        if (saved) {
            tieringManager.trackMessageLocation(message.messageId, StorageTier.HOT)
        }
        return saved
    }
    
    /**
     * 메시지 일괄 저장
     */
    suspend fun saveBatch(messages: List<Message>): BatchResult {
        val result = hotStorage.saveBatch(messages)
        
        // 성공한 메시지들의 위치 추적
        messages.take(result.successful).forEach { message ->
            tieringManager.trackMessageLocation(message.messageId, StorageTier.HOT)
        }
        
        return result
    }
    
    /**
     * 메시지 조회 (모든 계층 검색)
     */
    suspend fun getMessage(messageId: String): Message? {
        // 1. 위치 정보로 빠른 조회 시도
        val location = tieringManager.getMessageLocation(messageId)
        if (location != null) {
            val storageService = getStorageService(location.tier)
            val message = storageService.getMessage(messageId)
            if (message != null) {
                // 접근했으므로 핫 승격 고려
                considerPromotion(messageId, location.tier)
                return message
            }
        }
        
        // 2. 전체 계층 순차 검색 (핫 → 웜 → 콜드)
        return hotStorage.getMessage(messageId)
            ?: warmStorage.getMessage(messageId)?.also { 
                // 웜에서 찾았으면 핫으로 승격
                tieringManager.promoteMessage(messageId, StorageTier.HOT)
            }
            ?: coldStorage.getMessage(messageId)?.also { 
                // 콜드에서 찾았으면 웜으로 승격
                tieringManager.promoteMessage(messageId, StorageTier.WARM)
            }
    }
    
    /**
     * FIFO 큐에서 가장 오래된 메시지 조회
     */
    suspend fun getOldestMessage(queue: Queue): Message? {
        val queueName = "${queue.tenantId}:${queue.queueName}"
        
        return when (queue.queueType) {
            QueueType.FIFO -> {
                // FIFO: 모든 계층을 순서대로 검색하여 순서 보장
                hotStorage.getOldestMessage(queueName)
                    ?: warmStorage.getOldestMessage(queueName)?.also { 
                        tieringManager.promoteMessage(it.messageId, StorageTier.HOT)
                    }
                    ?: coldStorage.getOldestMessage(queueName)?.also { 
                        tieringManager.promoteMessage(it.messageId, StorageTier.HOT)
                    }
            }
            QueueType.STANDARD -> {
                // Standard: 핫 스토리지 우선, 없으면 비동기 승격
                hotStorage.getOldestMessage(queueName) ?: run {
                    // 백그라운드에서 웜/콜드에서 핫으로 승격
                    scheduleAsyncPromotion(queueName)
                    null
                }
            }
        }
    }
    
    /**
     * 여러 메시지 조회
     */
    suspend fun getMessages(queue: Queue, maxCount: Int): List<Message> {
        val queueName = "${queue.tenantId}:${queue.queueName}"
        val messages = mutableListOf<Message>()
        var remaining = maxCount
        
        when (queue.queueType) {
            QueueType.FIFO -> {
                // FIFO: 순서 보장을 위해 계층별로 순차 처리
                if (remaining > 0) {
                    val hotMessages = hotStorage.getMessages(queueName, remaining)
                    messages.addAll(hotMessages)
                    remaining -= hotMessages.size
                }
                
                if (remaining > 0) {
                    val warmMessages = warmStorage.getMessages(queueName, remaining)
                    messages.addAll(warmMessages)
                    // 웜에서 가져온 메시지들을 핫으로 승격
                    warmMessages.forEach { 
                        tieringManager.promoteMessage(it.messageId, StorageTier.HOT)
                    }
                    remaining -= warmMessages.size
                }
                
                if (remaining > 0) {
                    val coldMessages = coldStorage.getMessages(queueName, remaining)
                    messages.addAll(coldMessages)
                    // 콜드에서 가져온 메시지들을 핫으로 승격
                    coldMessages.forEach { 
                        tieringManager.promoteMessage(it.messageId, StorageTier.HOT)
                    }
                }
            }
            QueueType.STANDARD -> {
                // Standard: 핫 스토리지에서 최대한 가져오기
                val hotMessages = hotStorage.getMessages(queueName, maxCount)
                messages.addAll(hotMessages)
                
                if (hotMessages.size < maxCount) {
                    // 부족한 만큼 백그라운드 승격 스케줄링
                    scheduleAsyncPromotion(queueName)
                }
            }
        }
        
        return messages
    }
    
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
     * 메시지 일괄 삭제
     */
    suspend fun deleteBatch(messageIds: List<String>): BatchResult {
        val results = mutableListOf<BatchResult>()
        
        // 계층별로 그룹화하여 배치 삭제
        val messagesByTier = messageIds.groupBy { messageId ->
            tieringManager.getMessageLocation(messageId)?.tier ?: StorageTier.HOT
        }
        
        messagesByTier.forEach { (tier, ids) ->
            val storageService = getStorageService(tier)
            val result = storageService.deleteBatch(ids)
            results.add(result)
        }
        
        return results.fold(BatchResult(0, 0)) { acc, result ->
            BatchResult(
                successful = acc.successful + result.successful,
                failed = acc.failed + result.failed,
                errors = acc.errors + result.errors
            )
        }
    }
    
    /**
     * 큐 퍼지 (모든 메시지 삭제)
     */
    suspend fun purgeQueue(queue: Queue) {
        val queueName = "${queue.tenantId}:${queue.queueName}"
        
        // 모든 계층에서 해당 큐의 메시지 삭제
        // 실제 구현에서는 각 스토리지 서비스에 purgeQueue 메서드 추가 필요
        // 여기서는 간단히 표현
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
    
    private suspend fun considerPromotion(messageId: String, currentTier: StorageTier) {
        // 접근 패턴에 따른 자동 승격 고려
        when (currentTier) {
            StorageTier.COLD -> {
                // 콜드에서 접근하면 웜으로 승격
                tieringManager.promoteMessage(messageId, StorageTier.WARM)
            }
            StorageTier.WARM -> {
                // 웜에서 자주 접근하면 핫으로 승격 (접근 빈도 기반)
                // 실제로는 접근 패턴 분석 필요
            }
            StorageTier.HOT -> {
                // 이미 핫 스토리지에 있음
            }
        }
    }
    
    private suspend fun scheduleAsyncPromotion(queueName: String) {
        // 비동기로 웜/콜드에서 핫으로 메시지 승격
        tieringManager.performPredictivePromotion(queueName)
    }
}

/**
 * 큐 타입 (spec.md에서 정의된 대로)
 */
enum class QueueType {
    FIFO,     // 순서 보장
    STANDARD  // 최적 성능
}

/**
 * Queue 도메인 확장을 위한 확장 프로퍼티
 */
val Queue.queueType: QueueType
    get() = if (this.attributes.fifoQueue == true) QueueType.FIFO else QueueType.STANDARD
