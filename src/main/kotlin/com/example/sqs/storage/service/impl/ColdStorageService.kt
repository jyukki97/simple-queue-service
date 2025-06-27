package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.StorageTierService
import com.example.sqs.storage.service.StorageUsageInfo
import com.example.sqs.storage.service.cold.*
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Service
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock

/**
 * 콜드 스토리지 구현 (Local File System)
 * HOT-WARM-COLD 아키텍처의 COLD 계층
 *
 * 철학:
 * - 단순성: 파일 기반 순수 저장
 * - 안정성: 배치 쓰기, 압축, 무결성 검증
 * - 비용 효율성: 압축을 통한 스토리지 최적화
 * - 배치/아카이빙: 대량 처리에 최적화
 */
@Service
class ColdStorageService(
    private val fileUtils: ColdStorageFileUtils,
    private val integrityService: ColdStorageIntegrityService,
    private val compressionService: ColdStorageCompressionService,
    private val batchService: ColdStorageBatchService,
    private val queueService: ColdStorageQueueService,
    @Value("\${app.storage.cold.base-path:./data/cold}")
    private val basePath: String
) : StorageTierService {

    override val tier = StorageTier.COLD

    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()
    private val logger = LoggerFactory.getLogger(ColdStorageService::class.java)

    @PostConstruct
    fun init() {
        try {
            Files.createDirectories(Paths.get(basePath))
            logger.info("COLD 스토리지 초기화 완료: $basePath")
        } catch (e: Exception) {
            logger.error("COLD 스토리지 초기화 실패", e)
        }
    }

    override suspend fun saveMessage(message: Message): Boolean {
        return try {
            val result = saveBatch(listOf(message))
            result.successCount > 0
        } catch (e: Exception) {
            logger.error("메시지 저장 실패: ${message.messageId}", e)
            false
        }
    }

    override suspend fun saveBatch(messages: List<Message>): BatchResult {
        return withContext(Dispatchers.IO) {
            try {
                batchService.saveBatch(basePath, messages)
            } catch (e: Exception) {
                logger.error("배치 저장 실패", e)
                BatchResult(
                    successCount = 0,
                    failCount = messages.size,
                    errors = listOf("배치 저장 실패: ${e.message}")
                )
            }
        }
    }

    override suspend fun getMessage(messageId: String): Message? {
        return withContext(Dispatchers.IO) {
            try {
                fileUtils.searchMessageAcrossFiles(basePath, messageId)
            } catch (e: Exception) {
                logger.error("메시지 조회 실패: $messageId", e)
                null
            }
        }
    }

    /**
     * 특정 큐의 메시지 수를 조회 (파티션 포함)
     */
    fun getMessageCountByQueue(tenantId: String, queueName: String): Int {
        return queueService.getMessageCountByQueue(basePath, tenantId, queueName)
    }

    override suspend fun getOldestMessage(queueName: String): Message? {
        return withContext(Dispatchers.IO) {
            try {
                val messages = queueService.getMessages(basePath, queueName, 1)
                messages.firstOrNull()
            } catch (e: Exception) {
                logger.error("가장 오래된 메시지 조회 실패: $queueName", e)
                null
            }
        }
    }

    /**
     * 특정 큐의 가장 오래된 메시지 시간을 조회 (파티션 포함)
     */
    fun getOldestMessageTime(tenantId: String, queueName: String): Instant? {
        return queueService.getOldestMessageTime(basePath, tenantId, queueName)
    }

    override suspend fun getMessages(queueName: String, maxCount: Int): List<Message> {
        return withContext(Dispatchers.IO) {
            try {
                queueService.getMessages(basePath, queueName, maxCount)
            } catch (e: Exception) {
                logger.error("메시지 목록 조회 실패: $queueName", e)
                emptyList()
            }
        }
    }

    override suspend fun deleteMessage(messageId: String): Boolean {
        // COLD 스토리지는 아카이빙 목적이므로 삭제를 지원하지 않음
        logger.warn("COLD 스토리지는 삭제를 지원하지 않습니다: $messageId")
        return false
    }

    override suspend fun deleteBatch(messageIds: List<String>): BatchResult {
        logger.warn("COLD 스토리지는 일괄 삭제를 지원하지 않습니다: ${messageIds.size}개 메시지")
        return BatchResult(
            successCount = 0,
            failCount = messageIds.size,
            errors = listOf("COLD 스토리지는 삭제를 지원하지 않습니다")
        )
    }

    /**
     * 큐의 모든 메시지 삭제 (디렉토리 전체 삭제)
     */
    suspend fun deleteQueueMessages(tenantId: String, queueName: String): Boolean {
        return queueService.deleteQueueMessages(basePath, tenantId, queueName)
    }


    override suspend fun getExpiredMessages(maxAge: Duration, limit: Int): List<Message> {
        return withContext(Dispatchers.IO) {
            try {
                val cutoffTime = Instant.now().minus(maxAge)
                val expiredMessages = mutableListOf<Message>()

                // 오래된 날짜부터 검색
                val startDate = LocalDate.now().minusDays(maxAge.toDays() + 1)
                var searchDate = startDate

                while (!searchDate.isAfter(LocalDate.now()) && expiredMessages.size < limit) {
                    val archivePath = fileUtils.createArchivePath(basePath, searchDate)

                    if (Files.exists(archivePath)) {
                        Files.list(archivePath).use { stream ->
                            val files = stream.filter { it.toString().endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION) }.toList()
                            for (file in files) {
                                val messages = fileUtils.readMessagesFromFile(file)
                                val expired = messages.filter { it.sentTimestamp.isBefore(cutoffTime) }
                                expiredMessages.addAll(expired)

                                if (expiredMessages.size >= limit) {
                                    return@withContext expiredMessages.take(limit)
                                }
                            }
                        }
                    }

                    searchDate = searchDate.plusDays(1)
                }

                expiredMessages.take(limit)
            } catch (e: Exception) {
                logger.error("만료된 메시지 조회 실패", e)
                emptyList()
            }
        }
    }

    override suspend fun getUsageInfo(): StorageUsageInfo {
        return withContext(Dispatchers.IO) {
            try {
                val basePath = Paths.get(basePath)
                var totalSize = 0L
                var totalMessages = 0

                Files.walk(basePath)
                    .filter { it.toString().endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION) }
                    .forEach { file ->
                        totalSize += Files.size(file)
                        totalMessages += fileUtils.countMessagesInFile(file)
                    }

                val fileStore = Files.getFileStore(basePath)
                val totalCapacity = fileStore.totalSpace
                val availableSpace = fileStore.usableSpace
                val usedSpace = totalCapacity - availableSpace
                val usagePercent = if (totalCapacity > 0) (usedSpace.toDouble() / totalCapacity) * 100 else 0.0

                StorageUsageInfo(
                    tier = StorageTier.COLD,
                    totalCapacityBytes = totalCapacity,
                    usedBytes = usedSpace,
                    availableBytes = availableSpace,
                    usagePercent = usagePercent,
                    messageCount = totalMessages.toLong()
                )
            } catch (e: Exception) {
                logger.error("사용량 정보 조회 실패", e)
                StorageUsageInfo(
                    tier = StorageTier.COLD,
                    totalCapacityBytes = 0L,
                    usedBytes = 0L,
                    availableBytes = 0L,
                    usagePercent = 0.0,
                    messageCount = 0L
                )
            }
        }
    }

    override suspend fun getMetrics(): TierMetrics {
        val usageInfo = getUsageInfo()
        return TierMetrics(
            tier = StorageTier.COLD,
            messageCount = usageInfo.messageCount,
            storageUsageBytes = usageInfo.usedBytes,
            avgAccessTimeMs = 100.0, // COLD는 상대적으로 느림
            hitRate = 0.95, // 95% 정도의 성공률
            migrationRate = 0.1, // 낮은 이주율
            errorRate = 0.01, // 1% 오류율
            lastUpdated = Instant.now()
        )
    }

    // ============ Phase 2: 벤치마크용 메소드들 ============

    /**
     * Phase 2 기본 배치 저장 (체크섬 포함)
     */
    suspend fun saveBatchWithChecksum(messages: List<Message>): BatchResult {
        val startTime = System.currentTimeMillis()
        val result = saveBatch(messages) // 이미 Phase 2 기능 포함
        val duration = System.currentTimeMillis() - startTime

        logger.info("Phase2 체크섬 배치 저장: ${messages.size}개 메시지, 소요시간: ${duration}ms")
        return result
    }

    /**
     * 동적 압축 레벨 테스트
     */
    suspend fun testDynamicCompression(messages: List<Message>): Map<String, Any> {
        return batchService.testDynamicCompression(messages)
    }

    /**
     * 무결성 검증 상태 조회
     */
    fun getIntegrityStatus(): Map<String, Any> {
        return integrityService.getIntegrityStatus() + compressionService.getCompressionInfo()
    }
}
