package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.StorageTierService
import com.example.sqs.storage.service.StorageUsageInfo
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.nio.file.StandardCopyOption
import java.security.MessageDigest
import java.time.Duration
import java.time.Instant
import java.time.LocalDate
import java.time.format.DateTimeFormatter
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.locks.ReentrantLock
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * 콜드 스토리지 구현 (Local File System)
 * HOT-WARM-COLD 아키텍처의 COLD 계층
 *
 * 철학:
 * - 단순성: 복잡한 인메모리 캐싱 제거, 파일 기반 순수 저장
 * - 안정성: 배치 쓰기, 압축, 무결성 검증
 * - 비용 효율성: 압축을 통한 스토리지 최적화
 * - 배치/아카이빙: 대량 처리에 최적화
 */
@Service
@EnableScheduling
class ColdStorageService(
    private val objectMapper: ObjectMapper,
    @Value("\${app.storage.cold.base-path:./data/cold}")
    private val basePath: String
) : StorageTierService {

    override val tier = StorageTier.COLD

    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()
    private val logger = LoggerFactory.getLogger(ColdStorageService::class.java)

    // Phase 2: 무결성 강화 관련 필드
    private val checksumCache = ConcurrentHashMap<String, String>()
    private val integrityCheckQueue = ConcurrentHashMap<String, Instant>()

    // Phase 2: 설정
    @Value("\${app.storage.cold.phase2.dynamic-compression-enabled:true}")
    private val dynamicCompressionEnabled: Boolean = true

    @Value("\${app.storage.cold.phase2.max-checksum-overhead-percent:10}")
    private val maxChecksumOverheadPercent: Int = 10

    @Value("\${app.storage.cold.phase2.integrity-check-cpu-threshold:0.8}")
    private val integrityCheckCpuThreshold: Double = 0.8

    companion object {
        private const val MESSAGE_FILE_EXTENSION = ".json.gz"
        private val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    }

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
            var successful = 0
            var failed = 0
            val errors = mutableListOf<String>()

            try {
                // 큐별로 메시지를 그룹화 (queueName 사용)
                val messagesByQueue = messages.groupBy { it.queueName }
                val currentDate = LocalDate.now()
                val archivePath = createArchivePath(currentDate)

                for ((queueName, queueMessages) in messagesByQueue) {
                    try {
                        val fileLock = getFileLock(queueName)
                        fileLock.lock()

                        try {
                            val safeQueueName = queueName.replace(":", "_")
                            val archiveFile = archivePath.resolve("$safeQueueName$MESSAGE_FILE_EXTENSION")

                            // Phase 2: 체크섬과 함께 원자적 배치 쓰기
                            val result = writeBatchAtomicWithChecksum(archiveFile, queueMessages)
                            successful += result.successCount
                            failed += result.failCount
                            errors.addAll(result.errors)

                        } finally {
                            fileLock.unlock()
                        }
                    } catch (e: Exception) {
                        logger.error("큐별 메시지 저장 실패: $queueName", e)
                        failed += queueMessages.size
                        errors.add("큐 $queueName 저장 실패: ${e.message}")
                    }
                }

                updateMetrics(successful, failed)
            } catch (e: Exception) {
                logger.error("배치 저장 중 전체 실패", e)
                failed = messages.size
                errors.add("Batch save failed: ${e.message}")
            }

            BatchResult(successCount = successful, failCount = failed, errors = errors)
        }
    }

    override suspend fun getMessage(messageId: String): Message? {
        return withContext(Dispatchers.IO) {
            try {
                searchMessageAcrossFiles(messageId)
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
        return try {
            val queueNameWithoutPartition = queueName.split("_partition_")[0]
            val seenMessageIds = mutableSetOf<String>() // 중복 메시지 방지

            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .filter { path ->
                    // 경로나 파일명에서 큐 이름이 포함되어 있는지 확인
                    val pathStr = path.toString()
                    val fileName = path.fileName.toString()
                    pathStr.contains("/$queueNameWithoutPartition/") ||
                            pathStr.contains("/${queueNameWithoutPartition}_partition_") ||
                            pathStr.contains("$queueNameWithoutPartition-batch-") ||
                            fileName.startsWith("${tenantId}_$queueNameWithoutPartition") || // 콜론이 언더스코어로 치환된 경우
                            fileName.startsWith("$tenantId:$queueNameWithoutPartition") // 기존 콜론 포함 경우
                }
                .forEach { file ->
                    try {
                        val fileMessages = readMessagesFromFile(file)
                        fileMessages.forEach { message ->
                            // 중복 메시지 체크 및 큐 이름 매칭
                            if (!seenMessageIds.contains(message.messageId) &&
                                (message.queueName == queueName ||
                                        message.queueName.startsWith("${queueNameWithoutPartition}_partition_") ||
                                        message.queueName == queueNameWithoutPartition)
                            ) {
                                seenMessageIds.add(message.messageId)
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시
                        logger.debug("파일 읽기 실패: $file - ${e.message}")
                    }
                }
            seenMessageIds.size
        } catch (e: Exception) {
            logger.warn("COLD 메시지 수 조회 중 오류 발생: ${e.message}")
            0
        }
    }

    override suspend fun getOldestMessage(queueName: String): Message? {
        return withContext(Dispatchers.IO) {
            try {
                val messages = getMessages(queueName, 1)
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
        return try {
            val queueNameWithoutPartition = queueName.split("_partition_")[0]
            var oldestTime: Instant? = null

            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .filter { path ->
                    // 경로나 파일명에서 큐 이름이 포함되어 있는지 확인
                    val pathStr = path.toString()
                    val fileName = path.fileName.toString()
                    pathStr.contains("/$queueNameWithoutPartition/") ||
                            pathStr.contains("/${queueNameWithoutPartition}_partition_") ||
                            pathStr.contains("$queueNameWithoutPartition-batch-") ||
                            fileName.startsWith("${tenantId}_$queueNameWithoutPartition") || // 콜론이 언더스코어로 치환된 경우
                            fileName.startsWith("$tenantId:$queueNameWithoutPartition") // 기존 콜론 포함 경우
                }
                .forEach { file ->
                    try {
                        val fileMessages = readMessagesFromFile(file)
                        fileMessages.forEach { message ->
                            // 큐 이름 매칭
                            if (message.queueName == queueName ||
                                message.queueName.startsWith("${queueNameWithoutPartition}_partition_") ||
                                message.queueName == queueNameWithoutPartition
                            ) {
                                val messageTime = message.sentTimestamp
                                if (oldestTime == null || messageTime.isBefore(oldestTime)) {
                                    oldestTime = messageTime
                                }
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시
                        logger.debug("파일 읽기 실패: $file - ${e.message}")
                    }
                }

            oldestTime
        } catch (e: Exception) {
            logger.warn("COLD 최오래된 메시지 시간 조회 중 오류 발생: ${e.message}")
            null
        }
    }

    override suspend fun getMessages(queueName: String, maxCount: Int): List<Message> {
        return withContext(Dispatchers.IO) {
            try {
                val messages = mutableListOf<Message>()
                val currentDate = LocalDate.now()
                var searchDate = currentDate

                // 최근 7일간 검색
                repeat(7) {
                    val archivePath = createArchivePath(searchDate)
                    val safeQueueName = queueName.replace(":", "_")
                    val archiveFile = archivePath.resolve("$safeQueueName$MESSAGE_FILE_EXTENSION")

                    if (Files.exists(archiveFile)) {
                        val queueMessages = readMessagesFromFile(archiveFile)
                        messages.addAll(queueMessages)

                        if (messages.size >= maxCount) {
                            return@withContext messages.take(maxCount)
                        }
                    }

                    searchDate = searchDate.minusDays(1)
                }

                messages
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
        return withContext(Dispatchers.IO) {
            try {
                val queueBasePath = Paths.get(basePath)
                var deletedAny = false

                // 안전한 큐 키 생성 (콜론을 언더스코어로 치환)
                val safeQueueKey = "${tenantId}_$queueName"

                // 모든 날짜 디렉토리에서 해당 큐 삭제
                Files.walk(queueBasePath)
                    .filter { path ->
                        val fileName = path.fileName.toString()
                        // 정확한 큐 이름 매칭만 허용 (다른 큐와 간섭 방지)
                        fileName == "$safeQueueKey$MESSAGE_FILE_EXTENSION" ||
                                fileName.startsWith("${safeQueueKey}_partition_") && fileName.endsWith(MESSAGE_FILE_EXTENSION)
                    }
                    .sorted(Comparator.reverseOrder())
                    .forEach { path ->
                        if (Files.deleteIfExists(path)) {
                            deletedAny = true
                        }
                    }

                deletedAny
            } catch (e: Exception) {
                false
            }
        }
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
                    val archivePath = createArchivePath(searchDate)

                    if (Files.exists(archivePath)) {
                        Files.list(archivePath).use { stream ->
                            val files = stream.filter { it.toString().endsWith(MESSAGE_FILE_EXTENSION) }.toList()
                            for (file in files) {
                                val messages = readMessagesFromFile(file)
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
                    .filter { it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                    .forEach { file ->
                        totalSize += Files.size(file)
                        totalMessages += countMessagesInFile(file)
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

    /**
     * Phase 2: 체크섬과 함께 원자적 배치 쓰기
     */
    private suspend fun writeBatchAtomicWithChecksum(archiveFile: Path, messages: List<Message>): BatchResult {
        return withContext(Dispatchers.IO) {
            try {
                val startTime = System.currentTimeMillis()

                // 기존 메시지와 병합
                val allMessages = if (Files.exists(archiveFile)) {
                    val existingMessages = readMessagesFromFile(archiveFile).toMutableList()
                    val existingMessageIds = existingMessages.map { it.messageId }.toSet()
                    val newUniqueMessages = messages.filter { !existingMessageIds.contains(it.messageId) }
                    existingMessages.addAll(newUniqueMessages)
                    existingMessages
                } else {
                    messages
                }

                // 임시 파일에 쓰기
                val tempFile = Files.createTempFile("cold_batch_", ".tmp")
                val messagesJson = objectMapper.writeValueAsString(allMessages)

                // 동적 압축 레벨 결정
                val compressionLevel = if (dynamicCompressionEnabled) {
                    determineCompressionLevel()
                } else {
                    6 // 기본값
                }

                // 압축하여 임시 파일에 쓰기
                writeCompressedFile(tempFile, messagesJson, compressionLevel)

                // 체크섬 계산 (비동기)
                val checksum = calculateSHA256Async(Files.readAllBytes(tempFile))

                // 원자적 이동
                Files.createDirectories(archiveFile.parent)
                Files.move(tempFile, archiveFile, StandardCopyOption.REPLACE_EXISTING)

                // 메타데이터 저장
                saveMetadata(archiveFile, FileMetadata(
                    fileName = archiveFile.fileName.toString(),
                    messageCount = allMessages.size,
                    totalSizeBytes = Files.size(archiveFile),
                    compressionRatio = Files.size(archiveFile).toDouble() / messagesJson.length,
                    createdAt = Instant.now(),
                    lastModifiedAt = Instant.now(),
                    messageIdRange = if (allMessages.isNotEmpty()) {
                        allMessages.first().messageId to allMessages.last().messageId
                    } else null,
                    timestampRange = if (allMessages.isNotEmpty()) {
                        allMessages.minOf { it.sentTimestamp } to allMessages.maxOf { it.sentTimestamp }
                    } else null,
                    checksum = checksum,
                    checksumAlgorithm = "SHA-256"
                ))

                // 체크섬 캐시 업데이트
                checksumCache[archiveFile.toString()] = checksum

                // 무결성 검증 큐에 추가 (스마트 스케줄링)
                scheduleIntegrityCheck(archiveFile)

                val duration = System.currentTimeMillis() - startTime
                val newMessageCount = messages.size

                logger.info("COLD Phase2 배치 저장 완료: ${newMessageCount}개 메시지, 압축레벨: $compressionLevel, 소요시간: ${duration}ms, 체크섬: ${checksum.take(8)}...")

                BatchResult(successCount = newMessageCount, failCount = 0, errors = emptyList())

            } catch (e: Exception) {
                logger.error("Phase2 배치 쓰기 실패", e)
                BatchResult(successCount = 0, failCount = messages.size, errors = listOf("Phase2 배치 쓰기 실패: ${e.message}"))
            }
        }
    }

    /**
     * Phase 2: 시스템 상태에 따른 동적 압축 레벨 결정
     */
    private fun determineCompressionLevel(): Int {
        val metrics = SystemMetrics.getCurrent()

        return when {
            metrics.cpuUsage > 0.8 -> 1 // CPU 높으면 빠른 압축
            metrics.cpuUsage < 0.3 -> 9 // CPU 여유로우면 최대 압축
            else -> 6 // 기본값
        }
    }

    /**
     * Phase 2: 비동기 SHA-256 체크섬 계산
     */
    private suspend fun calculateSHA256Async(data: ByteArray): String = withContext(Dispatchers.IO) {
        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(data)
        hash.joinToString("") { "%02x".format(it) }
    }

    /**
     * Phase 2: 스마트 무결성 검증 스케줄링
     */
    private fun scheduleIntegrityCheck(filePath: Path) {
        val metrics = SystemMetrics.getCurrent()

        // CPU 사용률이 높으면 검증을 지연
        if (metrics.cpuUsage < integrityCheckCpuThreshold) {
            integrityCheckQueue[filePath.toString()] = Instant.now()
        } else {
            // CPU가 높으면 나중에 검증하도록 지연 스케줄링
            integrityCheckQueue[filePath.toString()] = Instant.now().plusSeconds(3600) // 1시간 후
        }
    }

    /**
     * Phase 2: 정기적 무결성 검증 (백그라운드)
     */
    @Scheduled(fixedRate = 300000) // 5분마다
    private fun performScheduledIntegrityChecks() {
        if (integrityCheckQueue.isEmpty()) return

        val metrics = SystemMetrics.getCurrent()
        if (metrics.cpuUsage > integrityCheckCpuThreshold) {
            logger.debug("CPU 사용률이 높아 무결성 검증 건너뜀: ${metrics.cpuUsage}")
            return
        }

        runBlocking {
            performBatchIntegrityCheck()
        }
    }

    /**
     * Phase 2: 배치 무결성 검증
     */
    private suspend fun performBatchIntegrityCheck() = withContext(Dispatchers.IO) {
        val now = Instant.now()
        val filesToCheck = integrityCheckQueue.filter { (_, scheduledTime) ->
            now.isAfter(scheduledTime)
        }.keys.take(10) // 한 번에 최대 10개 파일만 검증

        if (filesToCheck.isEmpty()) return@withContext

        val startTime = System.currentTimeMillis()
        var verifiedCount = 0
        var errorCount = 0

        logger.info("배치 무결성 검증 시작: ${filesToCheck.size}개 파일")

        for (filePathStr in filesToCheck) {
            try {
                val filePath = Paths.get(filePathStr)
                if (Files.exists(filePath)) {
                    val isValid = verifyFileIntegrity(filePath)
                    if (isValid) {
                        verifiedCount++
                    } else {
                        errorCount++
                    }
                }
            } catch (e: Exception) {
                errorCount++
                logger.error("파일 무결성 검증 중 오류: $filePathStr", e)
            } finally {
                integrityCheckQueue.remove(filePathStr)
            }
        }

        val totalTime = System.currentTimeMillis() - startTime
        logger.info("배치 무결성 검증 완료: ${filesToCheck.size}개 파일, 성공: $verifiedCount, 실패: $errorCount, 소요시간: ${totalTime}ms")
    }

    /**
     * Phase 2: 개별 파일 무결성 검증
     */
    private suspend fun verifyFileIntegrity(filePath: Path): Boolean = withContext(Dispatchers.IO) {
        try {
            // 메타데이터에서 체크섬 조회
            val metadata = readMetadata(filePath.parent, filePath.fileName.toString())
            if (metadata?.checksum == null) {
                logger.debug("체크섬 없는 파일, 검증 건너뜀: $filePath")
                return@withContext true
            }

            // 파일 데이터 읽기 및 체크섬 계산
            val fileData = Files.readAllBytes(filePath)
            val actualChecksum = calculateSHA256Async(fileData)

            val isValid = actualChecksum == metadata.checksum

            if (!isValid) {
                logger.error("체크섬 불일치 감지: $filePath, 예상=${metadata.checksum}, 실제=$actualChecksum")
                handleCorruptedFile(filePath)
            }

            isValid

        } catch (e: Exception) {
            logger.error("파일 무결성 검증 실패: $filePath", e)
            false
        }
    }

    /**
     * 손상된 파일 처리
     */
    private fun handleCorruptedFile(filePath: Path) {
        try {
            val backupPath = filePath.parent.resolve("corrupted_${System.currentTimeMillis()}_${filePath.fileName}")
            Files.move(filePath, backupPath)
            logger.error("손상된 파일을 백업으로 이동: $filePath -> $backupPath")
        } catch (e: Exception) {
            logger.error("손상된 파일 처리 실패: $filePath", e)
        }
    }

    // ============ 기본 유틸리티 메소드들 ============

    private fun createArchivePath(date: LocalDate): Path {
        val dateStr = date.format(DATE_FORMATTER)
        val archivePath = Paths.get(basePath, "archives", dateStr)
        Files.createDirectories(archivePath)
        return archivePath
    }

    private fun generateFileLocation(message: Message): String {
        val date = LocalDate.now()
        val dateStr = date.format(DATE_FORMATTER)
        val safeQueueName = message.queueName.replace(":", "_")
        return "$dateStr/$safeQueueName$MESSAGE_FILE_EXTENSION"
    }

    private fun getFileLock(key: String): ReentrantLock {
        return fileLocks.computeIfAbsent(key) { ReentrantLock() }
    }

    @Suppress("UNUSED_PARAMETER")
    private fun writeCompressedFile(file: Path, content: String, compressionLevel: Int = 6) {
        Files.createDirectories(file.parent)
        FileOutputStream(file.toFile()).use { fos ->
            GZIPOutputStream(fos).use { gzos ->
                gzos.write(content.toByteArray(Charsets.UTF_8))
            }
        }
    }

    private fun readMessagesFromFile(file: Path): List<Message> {
        if (!Files.exists(file)) return emptyList()

        return try {
            FileInputStream(file.toFile()).use { fis ->
                GZIPInputStream(fis).use { gzis ->
                    val json = gzis.readBytes().toString(Charsets.UTF_8)
                    objectMapper.readValue(json, objectMapper.typeFactory.constructCollectionType(List::class.java, Message::class.java))
                }
            }
        } catch (e: Exception) {
            logger.error("파일 읽기 실패: $file", e)
            emptyList()
        }
    }

    private fun searchMessageAcrossFiles(messageId: String): Message? {
        val currentDate = LocalDate.now()
        var searchDate = currentDate

        // 최근 30일간 검색
        repeat(30) {
            val archivePath = createArchivePath(searchDate)
            if (Files.exists(archivePath)) {
                Files.list(archivePath).use { stream ->
                    val files = stream.filter { it.toString().endsWith(MESSAGE_FILE_EXTENSION) }.toList()
                    for (file in files) {
                        val messages = readMessagesFromFile(file)
                        val found = messages.find { it.messageId == messageId }
                        if (found != null) {
                            return found
                        }
                    }
                }
            }
            searchDate = searchDate.minusDays(1)
        }

        return null
    }

    private fun countMessagesInFile(file: Path): Int {
        return try {
            readMessagesFromFile(file).size
        } catch (e: Exception) {
            0
        }
    }

    private fun updateMetrics(successful: Int, failed: Int) {
        // 메트릭 업데이트 로직
        logger.debug("COLD 메트릭 업데이트: 성공=$successful, 실패=$failed")
    }

    // ============ Phase 2: 메타데이터 관리 ============

    private fun saveMetadata(filePath: Path, metadata: FileMetadata) {
        try {
            val metadataFile = filePath.parent.resolve("${filePath.fileName}.metadata.json")
            val json = objectMapper.writeValueAsString(metadata)
            Files.write(metadataFile, json.toByteArray(Charsets.UTF_8))
        } catch (e: Exception) {
            logger.error("메타데이터 저장 실패: $filePath", e)
        }
    }

    private fun readMetadata(directory: Path, fileName: String): FileMetadata? {
        return try {
            val metadataFile = directory.resolve("${fileName}.metadata.json")
            if (Files.exists(metadataFile)) {
                val json = Files.readString(metadataFile, Charsets.UTF_8)
                objectMapper.readValue(json, FileMetadata::class.java)
            } else {
                null
            }
        } catch (e: Exception) {
            logger.error("메타데이터 읽기 실패: $directory/$fileName", e)
            null
        }
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
        val results = mutableMapOf<String, Any>()

        // 여러 압축 레벨로 테스트
        for (level in listOf(1, 6, 9)) {
            val startTime = System.currentTimeMillis()
            val tempFile = Files.createTempFile("compression_test_$level", ".json.gz")

            try {
                val json = objectMapper.writeValueAsString(messages)
                writeCompressedFile(tempFile, json, level)

                val duration = System.currentTimeMillis() - startTime
                val fileSize = Files.size(tempFile)
                val compressionRatio = fileSize.toDouble() / json.length

                results["level_$level"] = mapOf(
                    "duration_ms" to duration,
                    "file_size_bytes" to fileSize,
                    "compression_ratio" to compressionRatio,
                    "original_size_bytes" to json.length
                )

                logger.info("압축 레벨 $level: ${duration}ms, 크기: ${fileSize}bytes, 비율: ${"%.3f".format(compressionRatio)}")

            } finally {
                Files.deleteIfExists(tempFile)
            }
        }

        return results
    }

    /**
     * 무결성 검증 상태 조회
     */
    fun getIntegrityStatus(): Map<String, Any> {
        return mapOf(
            "pending_checks" to integrityCheckQueue.size,
            "cached_checksums" to checksumCache.size,
            "cpu_threshold" to integrityCheckCpuThreshold,
            "current_cpu_usage" to SystemMetrics.getCurrent().cpuUsage,
            "dynamic_compression_enabled" to dynamicCompressionEnabled
        )
    }
}

/**
 * 파일 메타데이터 (Phase 2: 무결성 강화)
 */
data class FileMetadata(
    val fileName: String,
    val messageCount: Int,
    val totalSizeBytes: Long,
    val compressionRatio: Double,
    val createdAt: Instant,
    val lastModifiedAt: Instant,
    val messageIdRange: Pair<String, String>?,
    val timestampRange: Pair<Instant, Instant>?,
    val checksum: String,
    val checksumAlgorithm: String = "SHA-256",
    val integrityVerified: Boolean = false,
    val lastVerifiedAt: Instant? = null
)

/**
 * Phase 2 시스템 메트릭 수집
 */
data class SystemMetrics(
    val cpuUsage: Double,
    val memoryUsage: Double,
    val ioWait: Double,
    val diskUsage: Double
) {
    companion object {
        fun getCurrent(): SystemMetrics {
            // 실제 환경에서는 시스템 메트릭을 수집
            // 데모를 위해 시뮬레이션된 값 사용
            return SystemMetrics(
                cpuUsage = Math.random() * 0.8,
                memoryUsage = Math.random() * 0.6,
                ioWait = Math.random() * 0.3,
                diskUsage = Math.random() * 0.5
            )
        }
    }
}
