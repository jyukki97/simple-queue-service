package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.MessageLocation
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.StorageTierService
import com.example.sqs.storage.service.StorageUsageInfo
import com.fasterxml.jackson.databind.ObjectMapper
import jakarta.annotation.PostConstruct
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.EnableScheduling
import org.springframework.stereotype.Service
import java.io.FileInputStream
import java.io.FileOutputStream
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
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
 * 장기 보관, 감사, 분석용 데이터를 압축하여 파일로 저장
 */
@Service
@EnableScheduling
class ColdStorageService(
    private val objectMapper: ObjectMapper,
    @Value("\${app.storage.cold.base-path:./data/cold}")
    private val basePath: String
) : StorageTierService {

    override val tier = StorageTier.COLD

    private val metricsCache = ConcurrentHashMap<String, Any>()
    private val fileLocks = ConcurrentHashMap<String, ReentrantLock>()
    private val logger = LoggerFactory.getLogger(ColdStorageService::class.java)

    companion object {
        private const val MESSAGE_FILE_EXTENSION = ".json.gz"
        private const val INDEX_FILE_NAME = "index.json.gz"
        private const val METADATA_FILE_NAME = "metadata.json"
        private val DATE_FORMATTER = DateTimeFormatter.ofPattern("yyyy/MM/dd")
    }

    init {
        // 기본 디렉토리 생성
        createDirectoryIfNotExists(Paths.get(basePath))
    }

    override suspend fun saveMessage(message: Message): Boolean = withContext(Dispatchers.IO) {
        try {
            // 단일 메시지는 그대로 저장하되, 향후 배치 처리 개선 예정
            val result = saveBatch(listOf(message))
            result.successCount > 0
        } catch (e: Exception) {
            logger.error("COLD 저장 중 예외 발생: messageId=${message.messageId}", e)
            updateMetrics(0, 1)
            false
        }
    }

    override suspend fun saveBatch(messages: List<Message>): BatchResult = withContext(Dispatchers.IO) {
        var successful = 0
        var failed = 0
        val errors = mutableListOf<String>()

        try {
            // 날짜별로 그룹화
            val messagesByDate = messages.groupBy {
                LocalDate.ofInstant(it.sentTimestamp, java.time.ZoneId.systemDefault())
            }

            messagesByDate.forEach { (date, dayMessages) ->
                try {
                    val archivePath = getArchivePath(date)
                    createDirectoryIfNotExists(archivePath)

                    // 큐별로 다시 그룹화
                    val messagesByQueue = dayMessages.groupBy { "${it.tenantId}:${it.queueName}" }

                    messagesByQueue.forEach { (queueKey, queueMessages) ->
                        try {
                            // 파일명에서 콜론을 언더스코어로 치환하여 파일 시스템 호환성 확보
                            val safeQueueKey = queueKey.replace(":", "_")

                            // 기존 파일이 있으면 append, 없으면 새로 생성
                            val archiveFile = archivePath.resolve("$safeQueueKey$MESSAGE_FILE_EXTENSION")

                            // 파일별 락을 사용하여 동시성 보호 (읽기/쓰기 모두 보호)
                            val fileLock = fileLocks.computeIfAbsent(archiveFile.toString()) { ReentrantLock() }

                            var actualNewMessageCount = 0
                            try {
                                fileLock.lock()

                                val allMessages = if (Files.exists(archiveFile)) {
                                    // 기존 파일에서 메시지 읽어와서 병합 (중복 방지)
                                    try {
                                        val existingMessages = readMessagesFromFile(archiveFile).toMutableList()
                                        val existingMessageIds = existingMessages.map { it.messageId }.toSet()

                                        // 중복되지 않는 새 메시지만 추가
                                        val newUniqueMessages = queueMessages.filter { !existingMessageIds.contains(it.messageId) }
                                        actualNewMessageCount = newUniqueMessages.size

                                        if (newUniqueMessages.isNotEmpty()) {
                                            logger.info("COLD 기존 파일에 ${newUniqueMessages.size}개 메시지 추가 (기존: ${existingMessages.size}개, 중복 제외: ${queueMessages.size - newUniqueMessages.size}개) - $archiveFile")
                                            existingMessages.addAll(newUniqueMessages)
                                        } else {
                                            logger.info("COLD 모든 메시지가 중복이라 스킵 - $archiveFile")
                                        }

                                        existingMessages
                                    } catch (e: Exception) {
                                        // 기존 파일이 손상된 경우 새로 생성
                                        actualNewMessageCount = queueMessages.size
                                        queueMessages
                                    }
                                } else {
                                    actualNewMessageCount = queueMessages.size
                                    queueMessages
                                }

                                // 실제로 저장할 메시지가 있는 경우에만 파일 쓰기
                                if (allMessages.isNotEmpty() && actualNewMessageCount > 0) {
                                    val messagesJson = objectMapper.writeValueAsString(allMessages)
                                    writeCompressedFile(archiveFile, messagesJson)
                                }
                            } finally {
                                fileLock.unlock()
                            }

                            // 배치 인덱스 업데이트
                            updateBatchIndex(queueMessages)

                            successful += actualNewMessageCount
                            logger.info("큐 $queueKey COLD 저장 완료: ${actualNewMessageCount}개 메시지")
                        } catch (e: Exception) {
                            logger.error("큐 $queueKey 처리 실패", e)
                            failed += queueMessages.size
                            errors.add("Failed to archive queue $queueKey for date $date: ${e.message}")
                        }
                    }
                } catch (e: Exception) {
                    logger.error("날짜 $date 처리 실패", e)
                    failed += dayMessages.size
                    errors.add("Failed to process date $date: ${e.message}")
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

    override suspend fun getMessage(messageId: String): Message? = withContext(Dispatchers.IO) {
        try {
            // 인덱스에서 메시지 위치 찾기
            val location = findMessageLocation(messageId)
            if (location != null) {
                readMessageFromFile(location)
            } else {
                null
            }
        } catch (e: Exception) {
            null
        }
    }

    override suspend fun getOldestMessage(queueName: String): Message? = withContext(Dispatchers.IO) {
        try {
            // FIFO: 가장 오래된 메시지 찾기
            val messages = getQueueMessages(queueName, 1)
            messages.firstOrNull()
        } catch (e: Exception) {
            null
        }
    }

    override suspend fun getMessages(queueName: String, maxCount: Int): List<Message> = withContext(Dispatchers.IO) {
        try {
            getQueueMessages(queueName, maxCount)
        } catch (e: Exception) {
            emptyList()
        }
    }

    override suspend fun deleteMessage(messageId: String): Boolean = withContext(Dispatchers.IO) {
        try {
            val location = findMessageLocation(messageId)
            if (location != null) {
                // 파일에서 메시지 제거 (실제로는 마킹으로 처리)
                markMessageAsDeleted(messageId, location)
                updateMetrics(-1, 0)
                true
            } else {
                false
            }
        } catch (e: Exception) {
            false
        }
    }

    override suspend fun deleteBatch(messageIds: List<String>): BatchResult = withContext(Dispatchers.IO) {
        var successful = 0
        var failed = 0
        val errors = mutableListOf<String>()

        messageIds.forEach { messageId ->
            try {
                if (deleteMessage(messageId)) {
                    successful++
                } else {
                    failed++
                    errors.add("Message not found: $messageId")
                }
            } catch (e: Exception) {
                failed++
                errors.add("Error deleting message $messageId: ${e.message}")
            }
        }

        BatchResult(successCount = successful, failCount = failed, errors = errors)
    }

    override suspend fun getExpiredMessages(maxAge: Duration, limit: Int): List<Message> = withContext(Dispatchers.IO) {
        try {
            val cutoffTime = Instant.now().minus(maxAge)
            val expiredMessages = mutableListOf<Message>()

            // 날짜별 디렉토리를 순회하며 만료된 메시지 검색
            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .limit(limit.toLong())
                .forEach { file ->
                    try {
                        val messages = readMessagesFromFile(file)
                        messages.filter { it.sentTimestamp.isBefore(cutoffTime) }
                            .forEach { expiredMessages.add(it) }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시
                    }
                }

            expiredMessages.take(limit)
        } catch (e: Exception) {
            emptyList()
        }
    }

    override suspend fun getMetrics(): TierMetrics = withContext(Dispatchers.IO) {
        try {
            val messageCount = getMessageCount()
            val storageUsage = calculateStorageUsage()

            TierMetrics(
                tier = StorageTier.COLD,
                messageCount = messageCount,
                storageUsageBytes = storageUsage,
                avgAccessTimeMs = 500.0, // 500ms 기본값 (디스크 I/O)
                hitRate = 0.0,
                migrationRate = 0.0,
                errorRate = metricsCache["errorRate"] as? Double ?: 0.0,
                lastUpdated = Instant.now()
            )
        } catch (e: Exception) {
            createDefaultMetrics()
        }
    }

    override suspend fun getUsageInfo(): StorageUsageInfo = withContext(Dispatchers.IO) {
        try {
            val basePath = Paths.get(this@ColdStorageService.basePath)
            val totalSpace = Files.getFileStore(basePath).totalSpace
            val usableSpace = Files.getFileStore(basePath).usableSpace
            val usedSpace = totalSpace - usableSpace
            val storageUsage = calculateStorageUsage()

            StorageUsageInfo(
                tier = StorageTier.COLD,
                totalCapacityBytes = totalSpace,
                usedBytes = storageUsage,
                availableBytes = usableSpace,
                usagePercent = if (totalSpace > 0) (usedSpace.toDouble() / totalSpace) * 100 else 0.0,
                messageCount = getMessageCount()
            )
        } catch (e: Exception) {
            StorageUsageInfo(
                tier = StorageTier.COLD,
                totalCapacityBytes = 0,
                usedBytes = 0,
                availableBytes = 0,
                usagePercent = 0.0,
                messageCount = 0
            )
        }
    }

    /**
     * 아카이브 생성
     */
    suspend fun createArchive(queueName: String, messages: List<Message>): Path = withContext(Dispatchers.IO) {
        val date = LocalDate.now()
        val archivePath = getArchivePath(date)
        createDirectoryIfNotExists(archivePath.parent)

        val archiveFile = archivePath.resolve("$queueName-${System.currentTimeMillis()}$MESSAGE_FILE_EXTENSION")
        val messagesJson = objectMapper.writeValueAsString(messages)
        writeCompressedFile(archiveFile, messagesJson)

        archiveFile
    }

    /**
     * 승격 후보 조회
     */
    suspend fun getCandidatesForPromotion(queueName: String, timeWindow: Duration): List<Message> = withContext(Dispatchers.IO) {
        val startTime = Instant.now()
        val endTime = startTime.plus(timeWindow)

        getQueueMessages(queueName, Int.MAX_VALUE)
            .filter { it.sentTimestamp.isAfter(startTime) && it.sentTimestamp.isBefore(endTime) }
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

    /**
     * 손상된 파일들을 정리하는 메서드 (시작 시 호출)
     */
    @PostConstruct
    private fun cleanupCorruptedFiles() {
        try {
            val basePath = Paths.get(this.basePath)
            if (!Files.exists(basePath)) {
                return
            }

            Files.walk(basePath)
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .forEach { file ->
                    try {
                        // 파일 검증 시도
                        readCompressedFile(file)
                    } catch (e: Exception) {
                        logger.warn("손상된 파일 발견, 삭제: $file", e)
                        try {
                            Files.deleteIfExists(file)
                        } catch (deleteException: Exception) {
                            logger.error("손상된 파일 삭제 실패: $file", deleteException)
                        }
                    }
                }

            logger.info("손상된 파일 정리 완료")
        } catch (e: Exception) {
            logger.error("손상된 파일 정리 중 오류", e)
        }
    }

    private fun getArchivePath(date: LocalDate): Path {
        val dateStr = date.format(DATE_FORMATTER)
        return Paths.get(basePath, "archives", dateStr)
    }

    private fun createDirectoryIfNotExists(path: Path) {
        if (!Files.exists(path)) {
            Files.createDirectories(path)
        }
    }

    private fun writeCompressedFile(path: Path, content: String) {
        writeCompressedFileAtomic(path, content)
    }

    /**
     * 원자적 파일 쓰기: 임시 파일에 쓰고 성공 시 원본으로 이동
     * 파일 손상을 방지하기 위한 핵심 메서드
     */
    private fun writeCompressedFileAtomic(path: Path, content: String) {
        val tempFile = Paths.get("${path.toString()}.tmp")

        try {
            // 디렉토리 생성
            createDirectoryIfNotExists(path.parent)

            // 임시 파일에 압축하여 저장
            GZIPOutputStream(FileOutputStream(tempFile.toFile())).use { gzip ->
                gzip.write(content.toByteArray())
                gzip.flush() // 모든 데이터가 쓰였는지 확인
            }

            // 임시 파일이 제대로 작성되었는지 검증
            validateCompressedFile(tempFile)

            // 원자적 이동 (rename)
            Files.move(tempFile, path, java.nio.file.StandardCopyOption.REPLACE_EXISTING)
        } catch (e: Exception) {
            // 실패 시 임시 파일 정리
            try {
                Files.deleteIfExists(tempFile)
            } catch (cleanupException: Exception) {
                logger.warn("임시 파일 정리 실패: ${tempFile}", cleanupException)
            }

            logger.error("원자적 파일 쓰기 실패: ${path}", e)
            throw e
        }
    }

    /**
     * 압축 파일이 올바르게 작성되었는지 검증
     */
    private fun validateCompressedFile(path: Path) {
        try {
            GZIPInputStream(FileInputStream(path.toFile())).use { gzip ->
                gzip.bufferedReader().readText()
            }
        } catch (e: Exception) {
            throw IllegalStateException("압축 파일 검증 실패: ${path}", e)
        }
    }

    private fun readCompressedFile(path: Path): String {
        return try {
            GZIPInputStream(FileInputStream(path.toFile())).use { gzip ->
                gzip.bufferedReader().readText()
            }
        } catch (e: java.util.zip.ZipException) {
            logger.error("압축 파일 손상 감지: ${path} - ${e.message}")
            // 손상된 파일을 백업으로 이동
            val corruptedPath = Paths.get("${path.toString()}.corrupted.${System.currentTimeMillis()}")
            try {
                Files.move(path, corruptedPath)
                logger.warn("손상된 파일을 백업으로 이동: ${path} -> ${corruptedPath}")
            } catch (moveException: Exception) {
                logger.error("손상된 파일 이동 실패", moveException)
            }
            throw e
        } catch (e: Exception) {
            logger.error("압축 파일 읽기 실패: ${path}", e)
            throw e
        }
    }

    private fun readMessageFromFile(location: MessageLocation): Message? {
        return try {
            // 실제 구현에서는 location 정보를 바탕으로 파일에서 메시지 읽기
            // 여기서는 단순화
            null
        } catch (e: Exception) {
            null
        }
    }

    private fun readMessagesFromFile(path: Path): List<Message> {
        return try {
            val content = readCompressedFile(path)
            if (content.isBlank()) {
                return emptyList()
            }

            try {
                // 먼저 배열로 파싱 시도 (일반적인 경우)
                val messages: List<Message> = objectMapper.readValue(
                    content,
                    objectMapper.typeFactory.constructCollectionType(List::class.java, Message::class.java)
                )
                messages
            } catch (e: Exception) {
                try {
                    // 단일 메시지로 파싱 시도 (레거시 호환성)
                    val message = objectMapper.readValue(content, Message::class.java)
                    listOf(message)
                } catch (e2: Exception) {
                    logger.error("파일 파싱 실패: $path - 배열 파싱 오류: ${e.message}, 단일 메시지 파싱 오류: ${e2.message}")
                    emptyList()
                }
            }
        } catch (e: Exception) {
            logger.error("압축 파일 읽기 실패: $path - ${e.message}")
            emptyList()
        }
    }

    private fun getQueueMessages(queueName: String, maxCount: Int): List<Message> {
        val messages = mutableListOf<Message>()
        val seenMessageIds = mutableSetOf<String>() // 중복 메시지 방지

        try {
            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .filter { path ->
                    val fileName = path.fileName.toString()
                    // 정확한 큐 이름 매칭만 허용 (다른 큐와 간섭 방지)
                    // 파일명 패턴: tenant-1_queueName.json.gz 또는 tenant-1_queueName_partition_X.json.gz
                    fileName.startsWith("tenant-1_$queueName$MESSAGE_FILE_EXTENSION") ||
                            fileName.startsWith("tenant-1_${queueName}_partition_") && fileName.endsWith(MESSAGE_FILE_EXTENSION)
                }
                .forEach { file ->
                    if (messages.size >= maxCount) return@forEach

                    try {
                        val fileMessages = readMessagesFromFile(file)

                        fileMessages.forEach { message ->
                            // 중복 메시지 체크 및 정확한 큐 이름 매칭
                            if (messages.size < maxCount &&
                                !seenMessageIds.contains(message.messageId) &&
                                (message.queueName == queueName ||
                                        message.queueName.startsWith("${queueName}_partition_"))
                            ) {
                                messages.add(message)
                                seenMessageIds.add(message.messageId)
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시 (로그는 debug 레벨로)
                        logger.warn("파일 읽기 실패: $file - ${e.message}")
                    }
                }
        } catch (e: Exception) {
            logger.error("콜드 스토리지 접근 오류: ${e.message}", e)
        }

        return messages.sortedBy { it.sentTimestamp }
    }

    private fun findMessageLocation(messageId: String): MessageLocation? {
        // 실제 구현에서는 인덱스 파일을 사용하여 빠른 검색
        return null
    }

    private fun updateIndex(message: Message) {
        // 인덱스 파일 업데이트 로직
    }

    private fun updateBatchIndex(messages: List<Message>) {
        // 배치 인덱스 업데이트 로직
    }

    private fun markMessageAsDeleted(messageId: String, location: MessageLocation) {
        // 메시지 삭제 마킹 로직
    }

    private fun getMessageCount(): Long {
        return try {
            val seenMessageIds = mutableSetOf<String>() // 중복 메시지 방지

            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .forEach { file ->
                    try {
                        val fileMessages = readMessagesFromFile(file)
                        fileMessages.forEach { message ->
                            seenMessageIds.add(message.messageId)
                        }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시
                    }
                }

            seenMessageIds.size.toLong()
        } catch (e: Exception) {
            0L
        }
    }

    private fun calculateStorageUsage(): Long {
        return try {
            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) }
                .mapToLong { Files.size(it) }
                .sum()
        } catch (e: Exception) {
            0L
        }
    }

    private fun updateMetrics(messageCountDelta: Int, errorCountDelta: Int) {
        metricsCache["errorRate"] = (metricsCache["errorRate"] as? Double ?: 0.0) + errorCountDelta
    }

    private fun createDefaultMetrics(): TierMetrics {
        return TierMetrics(
            tier = StorageTier.COLD,
            messageCount = 0,
            storageUsageBytes = 0,
            avgAccessTimeMs = 500.0,
            hitRate = 0.0,
            migrationRate = 0.0,
            errorRate = 0.0,
            lastUpdated = Instant.now()
        )
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
}
