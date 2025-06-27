package com.example.sqs.storage.service.cold

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.cold.FileMetadata
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.StandardCopyOption
import java.time.Instant
import java.time.LocalDate

/**
 * 콜드 스토리지 배치 처리 서비스
 */
@Component
class ColdStorageBatchService(
    private val objectMapper: ObjectMapper,
    private val fileUtils: ColdStorageFileUtils,
    private val integrityService: ColdStorageIntegrityService,
    private val compressionService: ColdStorageCompressionService
) {
    private val logger = LoggerFactory.getLogger(ColdStorageBatchService::class.java)

    /**
     * Phase 2: 체크섬과 함께 원자적 배치 쓰기
     */
    suspend fun writeBatchAtomicWithChecksum(
        basePath: String,
        archiveFile: Path, 
        messages: List<Message>
    ): BatchResult {
        return withContext(Dispatchers.IO) {
            try {
                val startTime = System.currentTimeMillis()

                // 기존 메시지와 병합
                val allMessages = if (Files.exists(archiveFile)) {
                    val existingMessages = fileUtils.readMessagesFromFile(archiveFile).toMutableList()
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
                val compressionLevel = compressionService.determineCompressionLevel()

                // 압축하여 임시 파일에 쓰기
                fileUtils.writeCompressedFile(tempFile, messagesJson, compressionLevel)

                // 체크섬 계산 (비동기)
                val checksum = integrityService.calculateSHA256Async(Files.readAllBytes(tempFile))

                // 원자적 이동
                Files.createDirectories(archiveFile.parent)
                Files.move(tempFile, archiveFile, StandardCopyOption.REPLACE_EXISTING)

                // 메타데이터 저장
                val metadata = FileMetadata(
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
                )
                fileUtils.saveMetadata(archiveFile, metadata)

                // 체크섬 캐시 업데이트
                integrityService.updateChecksumCache(archiveFile.toString(), checksum)

                // 무결성 검증 큐에 추가 (스마트 스케줄링)
                integrityService.scheduleIntegrityCheck(archiveFile)

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
     * 배치 저장 메인 로직
     */
    suspend fun saveBatch(basePath: String, messages: List<Message>): BatchResult {
        return withContext(Dispatchers.IO) {
            var successful = 0
            var failed = 0
            val errors = mutableListOf<String>()

            try {
                // 큐별로 메시지를 그룹화 (queueName 사용)
                val messagesByQueue = messages.groupBy { it.queueName }
                val currentDate = LocalDate.now()
                val archivePath = fileUtils.createArchivePath(basePath, currentDate)

                for ((queueName, queueMessages) in messagesByQueue) {
                    try {
                        val safeQueueName = queueName.replace(":", "_")
                        val archiveFile = archivePath.resolve("$safeQueueName${ColdStorageFileUtils.MESSAGE_FILE_EXTENSION}")

                        // Phase 2: 체크섬과 함께 원자적 배치 쓰기
                        val result = writeBatchAtomicWithChecksum(basePath, archiveFile, queueMessages)
                        successful += result.successCount
                        failed += result.failCount
                        errors.addAll(result.errors)

                    } catch (e: Exception) {
                        logger.error("큐별 메시지 저장 실패: $queueName", e)
                        failed += queueMessages.size
                        errors.add("큐 $queueName 저장 실패: ${e.message}")
                    }
                }

            } catch (e: Exception) {
                logger.error("배치 저장 중 전체 실패", e)
                failed = messages.size
                errors.add("Batch save failed: ${e.message}")
            }

            BatchResult(successCount = successful, failCount = failed, errors = errors)
        }
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
                fileUtils.writeCompressedFile(tempFile, json, level)

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
}
