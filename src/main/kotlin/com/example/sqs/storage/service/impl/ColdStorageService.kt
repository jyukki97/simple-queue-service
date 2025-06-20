package com.example.sqs.storage.service.impl

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.MessageLocation
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.TierMetrics
import com.example.sqs.storage.service.StorageTierService
import com.example.sqs.storage.service.StorageUsageInfo
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.springframework.beans.factory.annotation.Value
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
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * 콜드 스토리지 구현 (Local File System)
 * 장기 보관, 감사, 분석용 데이터를 압축하여 파일로 저장
 */
@Service
class ColdStorageService(
    private val objectMapper: ObjectMapper,
    @Value("\${app.storage.cold.base-path:/tmp/sqs-cold-storage}")
    private val basePath: String
) : StorageTierService {

    override val tier = StorageTier.COLD

    private val metricsCache = ConcurrentHashMap<String, Any>()

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
            val messagePath = getMessagePath(message)
            createDirectoryIfNotExists(messagePath.parent)

            // 메시지를 압축하여 저장
            val messageJson = objectMapper.writeValueAsString(message)
            writeCompressedFile(messagePath, messageJson)

            // 인덱스 업데이트
            updateIndex(message)

            // 메트릭 업데이트
            updateMetrics(1, 0)

            true
        } catch (e: Exception) {
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
                    createDirectoryIfNotExists(archivePath.parent)

                    // 큐별로 다시 그룹화
                    val messagesByQueue = dayMessages.groupBy { "${it.tenantId}:${it.queueName}" }

                    messagesByQueue.forEach { (queueKey, queueMessages) ->
                        try {
                            val archiveFile = archivePath.resolve("$queueKey$MESSAGE_FILE_EXTENSION")
                            val messagesJson = objectMapper.writeValueAsString(queueMessages)
                            writeCompressedFile(archiveFile, messagesJson)

                            // 배치 인덱스 업데이트
                            updateBatchIndex(queueMessages)

                            successful += queueMessages.size
                        } catch (e: Exception) {
                            failed += queueMessages.size
                            errors.add("Failed to archive queue $queueKey for date $date: ${e.message}")
                        }
                    }
                } catch (e: Exception) {
                    failed += dayMessages.size
                    errors.add("Failed to process date $date: ${e.message}")
                }
            }

            updateMetrics(successful, failed)
        } catch (e: Exception) {
            failed = messages.size
            errors.add("Batch save failed: ${e.message}")
        }

        BatchResult(successful, failed, errors)
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

        BatchResult(successful, failed, errors)
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

    private fun getMessagePath(message: Message): Path {
        val date = LocalDate.ofInstant(message.sentTimestamp, java.time.ZoneId.systemDefault())
        val dateStr = date.format(DATE_FORMATTER)
        return Paths.get(basePath, dateStr, message.tenantId, message.queueName, "${message.messageId}$MESSAGE_FILE_EXTENSION")
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
        GZIPOutputStream(FileOutputStream(path.toFile())).use { gzip ->
            gzip.write(content.toByteArray())
        }
    }

    private fun readCompressedFile(path: Path): String {
        return GZIPInputStream(FileInputStream(path.toFile())).use { gzip ->
            gzip.bufferedReader().readText()
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
            objectMapper.readValue(content, objectMapper.typeFactory.constructCollectionType(List::class.java, Message::class.java))
        } catch (e: Exception) {
            emptyList()
        }
    }

    private fun getQueueMessages(queueName: String, maxCount: Int): List<Message> {
        val messages = mutableListOf<Message>()

        try {
            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .filter { it.toString().contains(queueName) }
                .forEach { file ->
                    try {
                        val fileMessages = readMessagesFromFile(file)
                        fileMessages.forEach { message ->
                            if (messages.size < maxCount && message.queueName == queueName) {
                                messages.add(message)
                            }
                        }
                    } catch (e: Exception) {
                        // 개별 파일 오류는 무시
                    }
                }
        } catch (e: Exception) {
            // 전체 오류 처리
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
            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(MESSAGE_FILE_EXTENSION) }
                .mapToLong { file ->
                    try {
                        readMessagesFromFile(file).size.toLong()
                    } catch (e: Exception) {
                        0L
                    }
                }
                .sum()
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
}
