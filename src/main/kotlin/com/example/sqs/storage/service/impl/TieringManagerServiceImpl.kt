package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.*
import com.example.sqs.storage.service.*
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.time.Duration
import java.time.Instant
import java.util.UUID
import java.util.concurrent.ConcurrentHashMap

/**
 * 계층화 매니저 구현
 * 핫/웜/콜드 스토리지 간의 자동 계층화를 관리
 */
@Service
class TieringManagerServiceImpl(
    private val hotStorage: HotStorageService,
    private val warmStorage: WarmStorageService,
    private val coldStorage: ColdStorageService,
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper
) : TieringManagerService {
    
    private val messageLocationCache = ConcurrentHashMap<String, MessageLocation>()
    private var currentPolicy = createDefaultPolicy()
    
    companion object {
        private const val LOCATION_KEY_PREFIX = "location:"
        private const val MIGRATION_HISTORY_PREFIX = "migration_history:"
        private const val POLICY_KEY = "tiering_policy"
        private const val METRICS_KEY = "tiering_metrics"
    }
    
    override suspend fun trackMessageLocation(messageId: String, tier: StorageTier) {
        try {
            val location = MessageLocation(
                messageId = messageId,
                tier = tier,
                lastAccessTime = Instant.now(),
                accessCount = 1
            )
            
            messageLocationCache[messageId] = location
            
            val locationKey = LOCATION_KEY_PREFIX + messageId
            val locationJson = objectMapper.writeValueAsString(location)
            
            redisTemplate.opsForValue()
                .set(locationKey, locationJson, Duration.ofDays(14))
                .awaitSingleOrNull()
        } catch (e: Exception) {
            // 로깅만 하고 진행
        }
    }
    
    override suspend fun getMessageLocation(messageId: String): MessageLocation? {
        return try {
            // 캐시에서 먼저 확인
            messageLocationCache[messageId] ?: run {
                // Redis에서 조회
                val locationKey = LOCATION_KEY_PREFIX + messageId
                val locationJson = redisTemplate.opsForValue()
                    .get(locationKey)
                    .awaitSingleOrNull()
                
                locationJson?.let { json ->
                    val location = objectMapper.readValue(json, MessageLocation::class.java)
                    messageLocationCache[messageId] = location
                    location
                }
            }
        } catch (e: Exception) {
            null
        }
    }
    
    override suspend fun migrateMessage(messageId: String, fromTier: StorageTier, toTier: StorageTier): Boolean {
        return try {
            val sourceStorage = getStorageService(fromTier)
            val targetStorage = getStorageService(toTier)
            
            // 소스에서 메시지 조회
            val message = sourceStorage.getMessage(messageId)
            if (message == null) {
                return false
            }
            
            // 타겟에 저장
            val saved = targetStorage.saveMessage(message)
            if (saved) {
                // 소스에서 삭제
                sourceStorage.deleteMessage(messageId)
                
                // 위치 정보 업데이트
                trackMessageLocation(messageId, toTier)
                
                // 이관 이력 기록
                recordMigration(messageId, fromTier, toTier, "auto_migration")
                
                true
            } else {
                false
            }
        } catch (e: Exception) {
            false
        }
    }
    
    override suspend fun migrateBatch(
        messages: List<Message>, 
        fromTier: StorageTier, 
        toTier: StorageTier,
        reason: String
    ): BatchResult {
        val sourceStorage = getStorageService(fromTier)
        val targetStorage = getStorageService(toTier)
        val batchId = UUID.randomUUID().toString()
        
        try {
            // 배치로 타겟에 저장
            val saveResult = targetStorage.saveBatch(messages)
            
            if (saveResult.successful > 0) {
                // 성공한 메시지들을 소스에서 삭제
                val successfulMessageIds = messages.take(saveResult.successful).map { it.messageId }
                sourceStorage.deleteBatch(successfulMessageIds)
                
                // 위치 정보 및 이관 이력 업데이트
                successfulMessageIds.forEach { messageId ->
                    trackMessageLocation(messageId, toTier)
                    recordMigration(messageId, fromTier, toTier, reason, batchId)
                }
            }
            
            return saveResult
        } catch (e: Exception) {
            return BatchResult(0, messages.size, listOf("Migration failed: ${e.message}"))
        }
    }
    
    @Scheduled(fixedRate = 300000) // 5분마다
    override suspend fun performAutoTiering() {
        try {
            // 1. 핫 → 웜 이관
            migrateHotToWarm()
            
            // 2. 웜 → 콜드 이관
            migrateWarmToCold()
            
            // 3. 용량 기반 강제 이관
            performCapacityBasedMigration()
            
        } catch (e: Exception) {
            // 오류 로깅
        }
    }
    
    private suspend fun migrateHotToWarm() {
        val threshold = currentPolicy.hotToWarmThreshold
        threshold.maxAgeSeconds?.let { maxAge ->
            val expiredMessages = hotStorage.getExpiredMessages(Duration.ofSeconds(maxAge))
            
            if (expiredMessages.isNotEmpty()) {
                val result = migrateBatch(expiredMessages, StorageTier.HOT, StorageTier.WARM, "time_based_migration")
                println("Hot→Warm migration: ${result.successful} successful, ${result.failed} failed")
            }
        }
    }
    
    private suspend fun migrateWarmToCold() {
        val threshold = currentPolicy.warmToColdThreshold
        threshold.maxAgeSeconds?.let { maxAge ->
            val expiredMessages = warmStorage.getExpiredMessages(Duration.ofSeconds(maxAge))
            
            if (expiredMessages.isNotEmpty()) {
                val result = migrateBatch(expiredMessages, StorageTier.WARM, StorageTier.COLD, "time_based_migration")
                println("Warm→Cold migration: ${result.successful} successful, ${result.failed} failed")
            }
        }
    }
    
    private suspend fun performCapacityBasedMigration() {
        val hotUsage = hotStorage.getUsageInfo()
        val threshold = currentPolicy.hotToWarmThreshold
        
        threshold.maxCapacityPercent?.let { maxPercent ->
            if (hotUsage.usagePercent > maxPercent) {
                // 강제 이관: 가장 오래된 메시지들을 이관
                val messages = hotStorage.getExpiredMessages(Duration.ofMinutes(30), 1000)
                if (messages.isNotEmpty()) {
                    migrateBatch(messages, StorageTier.HOT, StorageTier.WARM, "capacity_based_migration")
                }
            }
        }
    }
    
    override suspend fun promoteMessage(messageId: String, targetTier: StorageTier): Boolean {
        val location = getMessageLocation(messageId)
        if (location == null || location.tier == targetTier) {
            return false
        }
        
        return when {
            location.tier == StorageTier.COLD && targetTier == StorageTier.HOT -> {
                // 콜드 → 핫 승격
                migrateThroughWarm(messageId)
            }
            location.tier == StorageTier.COLD && targetTier == StorageTier.WARM -> {
                migrateMessage(messageId, StorageTier.COLD, StorageTier.WARM)
            }
            location.tier == StorageTier.WARM && targetTier == StorageTier.HOT -> {
                migrateMessage(messageId, StorageTier.WARM, StorageTier.HOT)
            }
            else -> false
        }
    }
    
    private suspend fun migrateThroughWarm(messageId: String): Boolean {
        // 콜드 → 웜 → 핫 순차 이관
        val warmMigration = migrateMessage(messageId, StorageTier.COLD, StorageTier.WARM)
        if (warmMigration) {
            return migrateMessage(messageId, StorageTier.WARM, StorageTier.HOT)
        }
        return false
    }
    
    override suspend fun performPredictivePromotion(queueName: String) {
        // 예측 기반 승격 로직 (추후 ML 모델 통합)
        // 현재는 간단한 휴리스틱 사용
        
        try {
            // 최근 접근 패턴 분석
            val recentAccess = analyzeRecentAccess(queueName)
            
            if (recentAccess.accessFrequency > 10.0) { // 임계값
                // 콜드에서 웜으로 승격할 후보 조회
                val candidates = coldStorage.getCandidatesForPromotion(queueName, Duration.ofHours(1))
                
                candidates.take(100).forEach { message ->
                    migrateMessage(message.messageId, StorageTier.COLD, StorageTier.WARM)
                }
            }
        } catch (e: Exception) {
            // 예측 승격 실패는 로깅만
        }
    }
    
    override suspend fun applyTieringPolicy(policy: TieringPolicy) {
        currentPolicy = policy
        
        try {
            val policyJson = objectMapper.writeValueAsString(policy)
            redisTemplate.opsForValue()
                .set(POLICY_KEY, policyJson)
                .awaitSingleOrNull()
        } catch (e: Exception) {
            // 정책 저장 실패 로깅
        }
    }
    
    override suspend fun getMigrationHistory(messageId: String): List<TierMigrationRecord> {
        return try {
            val historyKey = MIGRATION_HISTORY_PREFIX + messageId
            val historyJson = redisTemplate.opsForValue()
                .get(historyKey)
                .awaitSingleOrNull()
            
            historyJson?.let { json ->
                objectMapper.readValue(json, objectMapper.typeFactory.constructCollectionType(List::class.java, TierMigrationRecord::class.java))
            } ?: emptyList()
        } catch (e: Exception) {
            emptyList()
        }
    }
    
    override suspend fun getAllTierMetrics(): Map<StorageTier, TierMetrics> {
        return try {
            mapOf(
                StorageTier.HOT to hotStorage.getMetrics(),
                StorageTier.WARM to warmStorage.getMetrics(),
                StorageTier.COLD to coldStorage.getMetrics()
            )
        } catch (e: Exception) {
            emptyMap()
        }
    }
    
    private fun getStorageService(tier: StorageTier): StorageTierService {
        return when (tier) {
            StorageTier.HOT -> hotStorage
            StorageTier.WARM -> warmStorage
            StorageTier.COLD -> coldStorage
        }
    }
    
    private suspend fun recordMigration(
        messageId: String, 
        fromTier: StorageTier, 
        toTier: StorageTier, 
        reason: String,
        batchId: String? = null
    ) {
        try {
            val record = TierMigrationRecord(
                fromTier = fromTier,
                toTier = toTier,
                migratedAt = Instant.now(),
                reason = reason,
                batchId = batchId
            )
            
            val historyKey = MIGRATION_HISTORY_PREFIX + messageId
            val currentHistory = getMigrationHistory(messageId).toMutableList()
            currentHistory.add(record)
            
            // 최근 10개 기록만 유지
            val trimmedHistory = currentHistory.takeLast(10)
            
            val historyJson = objectMapper.writeValueAsString(trimmedHistory)
            redisTemplate.opsForValue()
                .set(historyKey, historyJson, Duration.ofDays(30))
                .awaitSingleOrNull()
        } catch (e: Exception) {
            // 이력 기록 실패는 로깅만
        }
    }
    
    private suspend fun analyzeRecentAccess(queueName: String): AccessPattern {
        // 간단한 접근 패턴 분석
        // 실제로는 더 정교한 분석 필요
        return AccessPattern(
            queueName = queueName,
            accessFrequency = 5.0, // 임시값
            peakHours = listOf(9, 10, 14, 15),
            avgMessageLifetime = Duration.ofHours(2),
            readWriteRatio = 3.0,
            burstPattern = null
        )
    }
    
    private fun createDefaultPolicy(): TieringPolicy {
        return TieringPolicy(
            type = TieringPolicyType.TIME_BASED,
            hotToWarmThreshold = TieringThreshold(
                maxAgeSeconds = 3600, // 1시간
                maxCapacityPercent = 80
            ),
            warmToColdThreshold = TieringThreshold(
                maxAgeSeconds = 86400, // 24시간
                maxCapacityPercent = 90
            ),
            checkIntervalSeconds = 300,
            batchSize = 1000,
            enabled = true
        )
    }
}
