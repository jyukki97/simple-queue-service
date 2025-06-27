package com.example.sqs.storage.service.cold

import com.example.sqs.message.domain.Message
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Paths
import java.time.Instant
import java.time.LocalDate

/**
 * 콜드 스토리지 큐 검색 서비스
 */
@Component
class ColdStorageQueueService(
    private val fileUtils: ColdStorageFileUtils
) {
    private val logger = LoggerFactory.getLogger(ColdStorageQueueService::class.java)

    /**
     * 특정 큐의 메시지 수를 조회 (파티션 포함)
     */
    fun getMessageCountByQueue(basePath: String, tenantId: String, queueName: String): Int {
        return try {
            val queueNameWithoutPartition = queueName.split("_partition_")[0]
            val seenMessageIds = mutableSetOf<String>() // 중복 메시지 방지

            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION) }
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
                        val fileMessages = fileUtils.readMessagesFromFile(file)
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
    fun getOldestMessageTime(basePath: String, tenantId: String, queueName: String): Instant? {
        return try {
            val queueNameWithoutPartition = queueName.split("_partition_")[0]
            var oldestTime: Instant? = null

            Files.walk(Paths.get(basePath))
                .filter { Files.isRegularFile(it) && it.toString().endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION) }
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
                        val fileMessages = fileUtils.readMessagesFromFile(file)
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

    /**
     * 특정 큐의 메시지들을 조회
     */
    fun getMessages(basePath: String, queueName: String, maxCount: Int): List<Message> {
        return try {
            val messages = mutableListOf<Message>()
            val currentDate = LocalDate.now()
            var searchDate = currentDate
            
            // queueName이 tenantId:queueName 형태인지 queueName만인지 확인
            val (tenantId, actualQueueName) = if (queueName.contains(":")) {
                val parts = queueName.split(":")
                parts[0] to parts[1]
            } else {
                // queueName만 주어진 경우, 모든 파일을 검색해야 함
                null to queueName
            }

            // 최근 7일간 검색
            repeat(7) {
                val archivePath = fileUtils.createArchivePath(basePath, searchDate)
                
                if (Files.exists(archivePath)) {
                    if (tenantId != null) {
                        // tenantId가 있는 경우 특정 파일만 검색
                        val safeQueueName = queueName.replace(":", "_")
                        val archiveFile = archivePath.resolve("$safeQueueName${ColdStorageFileUtils.MESSAGE_FILE_EXTENSION}")
                        
                        if (Files.exists(archiveFile)) {
                            val queueMessages = fileUtils.readMessagesFromFile(archiveFile)
                            messages.addAll(queueMessages)
                        }
                    } else {
                        // tenantId가 없는 경우 모든 파일을 검색하여 매칭되는 메시지 찾기
                        Files.list(archivePath).use { stream ->
                            stream.filter { it.toString().endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION) }
                                .forEach { file ->
                                    try {
                                        val fileMessages = fileUtils.readMessagesFromFile(file)
                                        val matchingMessages = fileMessages.filter { message ->
                                            message.queueName == actualQueueName ||
                                            message.queueName.endsWith(":$actualQueueName") ||
                                            message.queueName.startsWith("${actualQueueName}_partition_")
                                        }
                                        messages.addAll(matchingMessages)
                                    } catch (e: Exception) {
                                        // 개별 파일 오류는 무시
                                        logger.debug("파일 읽기 실패: $file - ${e.message}")
                                    }
                                }
                        }
                    }
                    
                    if (messages.size >= maxCount) {
                        return messages.take(maxCount)
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

    /**
     * 큐의 모든 메시지 삭제 (디렉토리 전체 삭제)
     */
    suspend fun deleteQueueMessages(basePath: String, tenantId: String, queueName: String): Boolean {
        return try {
            val queueBasePath = Paths.get(basePath)
            var deletedAny = false

            // 안전한 큐 키 생성 (콜론을 언더스코어로 치환)
            val safeQueueKey = "${tenantId}_$queueName"

            // 모든 날짜 디렉토리에서 해당 큐 삭제
            Files.walk(queueBasePath)
                .filter { path ->
                    val fileName = path.fileName.toString()
                    // 정확한 큐 이름 매칭만 허용 (다른 큐와 간섭 방지)
                    fileName == "$safeQueueKey${ColdStorageFileUtils.MESSAGE_FILE_EXTENSION}" ||
                            fileName.startsWith("${safeQueueKey}_partition_") && fileName.endsWith(ColdStorageFileUtils.MESSAGE_FILE_EXTENSION)
                }
                .sorted(Comparator.reverseOrder())
                .forEach { path ->
                    if (Files.deleteIfExists(path)) {
                        deletedAny = true
                    }
                }

            deletedAny
        } catch (e: Exception) {
            logger.error("큐 메시지 삭제 실패: $queueName", e)
            false
        }
    }
}
