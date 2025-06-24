package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.WarmStorageStats
import com.fasterxml.jackson.databind.ObjectMapper
import org.rocksdb.RocksDB
import org.rocksdb.WriteBatch
import org.slf4j.LoggerFactory
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * RocksDB 기반 WARM 계층 스토리지 서비스
 *
 * 주요 기능:
 * - 메시지 저장/조회/삭제
 * - TTL 기반 만료 처리
 * - 배치 처리 지원
 * - 성능 모니터링
 */
@Service
class RocksDBWarmStorageService(
    private val rocksDB: RocksDB,
    private val objectMapper: ObjectMapper
) {

    private val logger = LoggerFactory.getLogger(RocksDBWarmStorageService::class.java)

    // TTL 관리를 위한 메모리 인덱스 (향후 RocksDB TTL 기능으로 대체 가능)
    private val ttlIndex = ConcurrentHashMap<String, Long>()

    companion object {
        private const val MESSAGE_PREFIX = "msg:"
        private const val TTL_PREFIX = "ttl:"
        private const val DEFAULT_TTL_HOURS = 24L
    }

    /**
     * 메시지 저장
     */
    fun saveMessage(message: Message, ttlHours: Long = DEFAULT_TTL_HOURS): Mono<Boolean> {
        return Mono.fromCallable {
            try {
                val messageKey = MESSAGE_PREFIX + message.messageId
                val messageJson = objectMapper.writeValueAsString(message)

                // 메시지 데이터 저장
                rocksDB.put(messageKey.toByteArray(), messageJson.toByteArray())

                // TTL 정보 저장
                val expiryTime = Instant.now().plusSeconds(ttlHours * 3600).epochSecond
                val ttlKey = TTL_PREFIX + message.messageId
                rocksDB.put(ttlKey.toByteArray(), expiryTime.toString().toByteArray())
                ttlIndex[message.messageId] = expiryTime

                logger.debug("RocksDB에 메시지 저장 완료: ${message.messageId}")
                true
            } catch (e: Exception) {
                logger.error("RocksDB 메시지 저장 실패: ${message.messageId}", e)
                false
            }
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 메시지 조회
     */
    fun getMessage(messageId: String): Mono<Message?> {
        return Mono.fromCallable {
            try {
                // TTL 확인
                if (isExpired(messageId)) {
                    logger.debug("메시지 만료됨: $messageId")
                    deleteMessage(messageId).subscribe()
                    return@fromCallable null
                }

                val messageKey = MESSAGE_PREFIX + messageId
                val messageBytes = rocksDB.get(messageKey.toByteArray())

                if (messageBytes != null) {
                    val messageJson = String(messageBytes)
                    objectMapper.readValue(messageJson, Message::class.java)
                } else {
                    null
                }
            } catch (e: Exception) {
                logger.error("RocksDB 메시지 조회 실패: $messageId", e)
                null
            }
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 메시지 삭제
     */
    fun deleteMessage(messageId: String): Mono<Boolean> {
        return Mono.fromCallable {
            try {
                val messageKey = MESSAGE_PREFIX + messageId
                val ttlKey = TTL_PREFIX + messageId

                rocksDB.delete(messageKey.toByteArray())
                rocksDB.delete(ttlKey.toByteArray())
                ttlIndex.remove(messageId)

                true
            } catch (e: Exception) {
                logger.error("RocksDB 메시지 삭제 실패: $messageId", e)
                false
            }
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 배치 메시지 저장
     */
    fun saveMessageBatch(messages: List<Message>, ttlHours: Long = DEFAULT_TTL_HOURS): Mono<BatchResult> {
        return Mono.fromCallable {
            var successCount = 0
            var failureCount = 0
            val errors = mutableListOf<String>()

            try {
                // WriteBatch 사용으로 성능 최적화
                val writeBatch = WriteBatch()
                val expiryTime = Instant.now().plusSeconds(ttlHours * 3600).epochSecond

                messages.forEach { message ->
                    try {
                        val messageKey = MESSAGE_PREFIX + message.messageId
                        val ttlKey = TTL_PREFIX + message.messageId
                        val messageJson = objectMapper.writeValueAsString(message)

                        writeBatch.put(messageKey.toByteArray(), messageJson.toByteArray())
                        writeBatch.put(ttlKey.toByteArray(), expiryTime.toString().toByteArray())

                        ttlIndex[message.messageId] = expiryTime
                        successCount++
                    } catch (e: Exception) {
                        failureCount++
                        errors.add("메시지 ${message.messageId} 저장 실패: ${e.message}")
                    }
                }

                // 배치 실행
                rocksDB.write(org.rocksdb.WriteOptions(), writeBatch)
                writeBatch.close()
            } catch (e: Exception) {
                logger.error("RocksDB 배치 저장 실패", e)
                errors.add("배치 저장 실패: ${e.message}")
            }

            BatchResult(successCount, failureCount, errors)
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 만료된 메시지 정리
     */
    fun cleanupExpiredMessages(): Mono<Int> {
        return Mono.fromCallable {
            var cleanedCount = 0
            val currentTime = Instant.now().epochSecond

            try {
                val expiredMessageIds = ttlIndex.entries
                    .filter { it.value <= currentTime }
                    .map { it.key }

                expiredMessageIds.forEach { messageId ->
                    deleteMessage(messageId).subscribe()
                    cleanedCount++
                }

                logger.info("만료된 메시지 정리 완료: $cleanedCount 개")

            } catch (e: Exception) {
                logger.error("만료된 메시지 정리 실패", e)
            }

            cleanedCount
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 큐별 메시지 목록 조회 (페이징 지원)
     */
    fun getMessagesByQueue(tenantId: String, queueName: String, limit: Int = 100, offset: Int = 0): Flux<Message> {
        return Mono.fromCallable {
            val messages = mutableListOf<Message>()

            try {
                // RocksDB 이터레이터 사용
                val iterator = rocksDB.newIterator()
                iterator.seekToFirst()

                var matchedCount = 0

                while (iterator.isValid() && messages.size < limit) {
                    val key = String(iterator.key())

                    // 메시지 키만 처리 (TTL 키는 제외)
                    if (key.startsWith(MESSAGE_PREFIX)) {
                        try {
                            val messageId = key.removePrefix(MESSAGE_PREFIX)

                            // TTL 확인
                            if (!isExpired(messageId)) {
                                val messageJson = String(iterator.value())
                                val message = objectMapper.readValue(messageJson, Message::class.java)

                                // 큐 필터링 - 정확한 큐 이름 또는 파티션된 큐 이름 매칭
                                // FIFO 큐: test-queue.fifo -> test-queue.fifo_partition_X
                                // Standard 큐: test-queue -> test-queue_partition_X
                                val isMatchingQueue = message.tenantId == tenantId &&
                                        (message.queueName == queueName ||
                                                message.queueName.startsWith("${queueName}_partition_") ||
                                                message.queueName.startsWith("${queueName.removeSuffix(".fifo")}_partition_") ||
                                                (queueName.endsWith(".fifo") && message.queueName.startsWith("${queueName}_partition_")))

                                if (isMatchingQueue) {
                                    // 오프셋 처리
                                    if (matchedCount >= offset) {
                                        messages.add(message)
                                    }
                                    matchedCount++
                                }
                            }
                        } catch (e: Exception) {
                            logger.error("메시지 파싱 실패: $key", e)
                        }
                    }

                    iterator.next()
                }

                iterator.close()

            } catch (e: Exception) {
                logger.error("큐별 메시지 조회 실패: $tenantId:$queueName", e)
            }

            messages
        }
            .flatMapMany { Flux.fromIterable(it) }
            .subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 저장소 통계 조회
     */
    fun getStorageStats(): Mono<WarmStorageStats> {
        return Mono.fromCallable {
            try {
                // 실제 RocksDB 키를 스캔하여 정확한 통계 계산
                val iterator = rocksDB.newIterator()
                var totalMessages = 0
                var expiredMessages = 0
                val now = Instant.now().epochSecond

                iterator.seekToFirst()
                while (iterator.isValid()) {
                    val key = String(iterator.key())

                    if (key.startsWith(MESSAGE_PREFIX)) {
                        totalMessages++
                        val messageId = key.substring(MESSAGE_PREFIX.length)

                        // TTL 확인
                        val ttlKey = TTL_PREFIX + messageId
                        val ttlBytes = rocksDB.get(ttlKey.toByteArray())
                        if (ttlBytes != null) {
                            val expiryTime = String(ttlBytes).toLongOrNull() ?: 0
                            if (expiryTime <= now) {
                                expiredMessages++
                            }
                        }
                    }
                    iterator.next()
                }
                iterator.close()

                val activeMessages = totalMessages - expiredMessages
                val estimatedSizeBytes = totalMessages * 1024L

                WarmStorageStats(
                    totalMessages = totalMessages,
                    activeMessages = activeMessages,
                    expiredMessages = expiredMessages,
                    estimatedSizeBytes = estimatedSizeBytes
                )
            } catch (e: Exception) {
                logger.error("RocksDB 통계 조회 실패", e)
                WarmStorageStats(0, 0, 0, 0)
            }
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * TTL 만료 확인
     */
    private fun isExpired(messageId: String): Boolean {
        val expiryTime = ttlIndex[messageId] ?: return false
        return expiryTime <= Instant.now().epochSecond
    }

    /**
     * 디버그: RocksDB의 모든 메시지 정보 조회
     */
    fun debugAllMessages(): Mono<List<Map<String, Any>>> {
        return Mono.fromCallable {
            val messageInfos = mutableListOf<Map<String, Any>>()

            try {
                val iterator = rocksDB.newIterator()
                iterator.seekToFirst()

                while (iterator.isValid()) {
                    val key = String(iterator.key())

                    if (key.startsWith(MESSAGE_PREFIX)) {
                        try {
                            val messageId = key.removePrefix(MESSAGE_PREFIX)
                            val messageJson = String(iterator.value())
                            val message = objectMapper.readValue(messageJson, Message::class.java)

                            messageInfos.add(
                                mapOf(
                                    "messageId" to messageId,
                                    "tenantId" to message.tenantId,
                                    "queueName" to message.queueName,
                                    "isExpired" to isExpired(messageId),
                                    "messageBodyStart" to message.messageBody.take(50)
                                )
                            )
                        } catch (e: Exception) {
                            messageInfos.add(
                                mapOf(
                                    "messageId" to key.removePrefix(MESSAGE_PREFIX),
                                    "error" to "파싱 실패: ${e.message}"
                                )
                            )
                        }
                    }

                    iterator.next()
                }

                iterator.close()

            } catch (e: Exception) {
                logger.error("디버그 모든 메시지 조회 실패", e)
            }

            messageInfos.toList()
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 디버그: 처음 몇 개 메시지의 큐 이름 확인
     */
    fun debugFirstFewMessages(count: Int = 5): Mono<List<Map<String, String>>> {
        return Mono.fromCallable {
            val messageInfos = mutableListOf<Map<String, String>>()

            try {
                logger.info("RocksDB 디버그 시작: 처음 ${count}개 메시지 조회")
                val iterator = rocksDB.newIterator()
                iterator.seekToFirst()

                var processedCount = 0
                while (iterator.isValid() && messageInfos.size < count && processedCount < 100) {
                    val key = String(iterator.key())
                    processedCount++

                    if (key.startsWith(MESSAGE_PREFIX)) {
                        try {
                            val messageJson = String(iterator.value())
                            val message = objectMapper.readValue(messageJson, Message::class.java)

                            messageInfos.add(
                                mapOf(
                                    "messageId" to message.messageId,
                                    "queueName" to message.queueName,
                                    "tenantId" to message.tenantId,
                                    "messageBody" to message.messageBody.take(50),
                                    "keyInDB" to key
                                )
                            )
                            logger.info("메시지 발견: ${message.messageId}, 큐: ${message.queueName}")
                        } catch (e: Exception) {
                            logger.warn("메시지 파싱 실패: $key", e)
                            messageInfos.add(
                                mapOf(
                                    "error" to "파싱 실패",
                                    "key" to key,
                                    "errorMessage" to (e.message ?: "알 수 없는 오류")
                                )
                            )
                        }
                    }

                    iterator.next()
                }

                iterator.close()
                logger.info("RocksDB 디버그 완료: ${messageInfos.size}개 메시지 조회됨")

            } catch (e: Exception) {
                logger.error("디버그 메시지 조회 실패", e)
                messageInfos.add(
                    mapOf(
                        "fatalError" to "RocksDB 접근 실패",
                        "errorMessage" to (e.message ?: "알 수 없는 오류")
                    )
                )
            }

            messageInfos.toList()
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 큐의 가장 오래된 메시지 ID들을 가져옴 (FIFO 순서)
     */
    fun getOldestMessageIds(tenantId: String, queueName: String, count: Int): Flux<String> {
        return Mono.fromCallable {
            try {
                val messageIds = mutableListOf<Pair<String, Long>>()

                val iterator = rocksDB.newIterator()
                iterator.use { iter ->
                    iter.seekToFirst()

                    while (iter.isValid()) {
                        val key = iter.key().toString(Charsets.UTF_8)

                        // MESSAGE_PREFIX로 시작하는 키만 처리
                        if (key.startsWith(MESSAGE_PREFIX)) {
                            try {
                                val messageJson = iter.value().toString(Charsets.UTF_8)
                                val message = objectMapper.readValue(messageJson, Message::class.java)

                                // 해당 큐의 메시지인지 확인 (파티션 고려)
                                // FIFO 큐: test-queue.fifo -> test-queue.fifo_partition_X
                                // Standard 큐: test-queue -> test-queue_partition_X
                                val isMatchingQueue = message.tenantId == tenantId &&
                                        (message.queueName == queueName ||
                                                message.queueName.startsWith("${queueName}_partition_") ||
                                                message.queueName.startsWith("${queueName.removeSuffix(".fifo")}_partition_") ||
                                                (queueName.endsWith(".fifo") && message.queueName.startsWith("${queueName}_partition_")))

                                if (isMatchingQueue) {
                                    val messageId = key.removePrefix(MESSAGE_PREFIX)
                                    messageIds.add(messageId to message.sentTimestamp.epochSecond)
                                }
                            } catch (e: Exception) {
                                logger.warn("메시지 파싱 실패: $key", e)
                            }
                        }

                        iter.next()
                    }
                }

                logger.info("큐 ${tenantId}:${queueName}에서 ${messageIds.size}개 메시지 발견, ${count}개 선택")

                // 오래된 순서로 정렬하여 count개만 반환
                messageIds.sortedBy { it.second }
                    .take(count)
                    .map { it.first }

            } catch (e: Exception) {
                logger.error("오래된 메시지 조회 실패: ${tenantId}:${queueName}", e)
                emptyList<String>()
            }
        }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapIterable { messageIdList -> messageIdList }
    }

    /**
     * 큐의 만료된 메시지 ID들을 가져옴
     */
    fun getExpiredMessageIds(tenantId: String, queueName: String, cutoffTime: Instant): Flux<String> {
        return Mono.fromCallable {
            try {
                val expiredIds = mutableListOf<String>()

                val iterator = rocksDB.newIterator()
                iterator.use { iter ->
                    iter.seekToFirst()

                    while (iter.isValid()) {
                        val key = iter.key().toString(Charsets.UTF_8)

                        // MESSAGE_PREFIX로 시작하는 키만 처리
                        if (key.startsWith(MESSAGE_PREFIX)) {
                            try {
                                val messageJson = iter.value().toString(Charsets.UTF_8)
                                val message = objectMapper.readValue(messageJson, Message::class.java)

                                // 해당 큐의 메시지이고 만료된 메시지인지 확인
                                // FIFO 큐: test-queue.fifo -> test-queue.fifo_partition_X
                                // Standard 큐: test-queue -> test-queue_partition_X
                                val isMatchingQueue = message.tenantId == tenantId &&
                                        (message.queueName == queueName ||
                                                message.queueName.startsWith("${queueName}_partition_") ||
                                                message.queueName.startsWith("${queueName.removeSuffix(".fifo")}_partition_") ||
                                                (queueName.endsWith(".fifo") && message.queueName.startsWith("${queueName}_partition_")))

                                if (isMatchingQueue && message.sentTimestamp.isBefore(cutoffTime)) {
                                    val messageId = key.removePrefix(MESSAGE_PREFIX)
                                    expiredIds.add(messageId)
                                }
                            } catch (e: Exception) {
                                logger.warn("메시지 파싱 실패: $key", e)
                            }
                        }

                        iter.next()
                    }
                }

                logger.info("큐 ${tenantId}:${queueName}에서 ${expiredIds.size}개 만료된 메시지 발견")
                expiredIds

            } catch (e: Exception) {
                logger.error("만료된 메시지 조회 실패: ${tenantId}:${queueName}", e)
                emptyList<String>()
            }
        }
            .subscribeOn(Schedulers.boundedElastic())
            .flatMapIterable { expiredIdList -> expiredIdList }
    }

    /**
     * 모든 메시지 ID 목록 조회 (내부용)
     */
    private fun getAllMessageIds(): List<String> {
        return ttlIndex.keys.toList()
    }

    /**
     * 큐별 모든 메시지 삭제
     */
    fun deleteQueueMessages(tenantId: String, queueName: String): Mono<Int> {
        return Mono.fromCallable {
            var deletedCount = 0

            try {
                // 큐에 속한 모든 메시지 찾기
                val iterator = rocksDB.newIterator()
                iterator.seekToFirst()

                val keysToDelete = mutableListOf<String>()

                while (iterator.isValid) {
                    val key = String(iterator.key())

                    if (key.startsWith(MESSAGE_PREFIX)) {
                        try {
                            val messageJson = String(iterator.value())
                            val message = objectMapper.readValue(messageJson, Message::class.java)

                            // 같은 테넌트이고 큐 이름이 일치하는지 확인 (파티셔닝 고려)
                            // FIFO 큐: test-queue.fifo -> test-queue.fifo_partition_X
                            // Standard 큐: test-queue -> test-queue_partition_X
                            val isMatchingQueue = message.tenantId == tenantId &&
                                    (message.queueName == queueName ||
                                            message.queueName.startsWith("${queueName}_partition_") ||
                                            message.queueName.startsWith("${queueName.removeSuffix(".fifo")}_partition_") ||
                                            (queueName.endsWith(".fifo") && message.queueName.startsWith("${queueName}_partition_")))

                            if (isMatchingQueue) {

                                keysToDelete.add(key)

                                // TTL 인덱스에서도 제거
                                val messageId = key.removePrefix(MESSAGE_PREFIX)
                                ttlIndex.remove(messageId)

                                deletedCount++
                            }
                        } catch (e: Exception) {
                            logger.warn("메시지 파싱 실패: $key", e)
                        }
                    }

                    iterator.next()
                }

                iterator.close()

                // 실제 키 삭제
                keysToDelete.forEach { key ->
                    rocksDB.delete(key.toByteArray())
                }

                if (deletedCount > 0) {
                    logger.info("WARM 계층에서 큐 ${tenantId}:${queueName} 메시지 ${deletedCount}개 삭제 완료")
                }

            } catch (e: Exception) {
                logger.error("큐 메시지 삭제 실패: ${tenantId}:${queueName}", e)
            }

            deletedCount
        }.subscribeOn(Schedulers.boundedElastic())
    }

    /**
     * 특정 큐의 메시지 개수 조회
     */
    fun getMessageCountByQueue(tenantId: String, queueName: String): Int {
        return try {
            val iterator = rocksDB.newIterator()
            iterator.seekToFirst()

            var count = 0
            while (iterator.isValid()) {
                val key = String(iterator.key())

                if (key.startsWith(MESSAGE_PREFIX)) {
                    val messageId = key.removePrefix(MESSAGE_PREFIX)
                    val messageBytes = iterator.value()

                    if (messageBytes != null) {
                        try {
                            val messageJson = String(messageBytes)
                            val messageData = objectMapper.readTree(messageJson)

                            val msgTenantId = messageData.get("tenantId")?.asText()
                            val msgQueueName = messageData.get("queueName")?.asText()

                            // 큐 이름 매칭 (파티션 포함)
                            if (msgTenantId == tenantId &&
                                (msgQueueName == queueName || msgQueueName?.startsWith("${queueName}_partition_") == true)
                            ) {
                                count++
                            }
                        } catch (e: Exception) {
                            logger.warn("메시지 파싱 중 오류: $messageId", e)
                        }
                    }
                }

                iterator.next()
            }

            iterator.close()
            count
        } catch (e: Exception) {
            logger.error("큐 메시지 개수 조회 실패: ${tenantId}:${queueName}", e)
            0
        }
    }

    /**
     * 특정 큐의 가장 오래된 메시지 시간 조회
     */
    fun getOldestMessageTime(tenantId: String, queueName: String): Instant? {
        return try {
            val iterator = rocksDB.newIterator()
            iterator.seekToFirst()

            var oldestTime: Instant? = null

            while (iterator.isValid()) {
                val key = String(iterator.key())

                if (key.startsWith(MESSAGE_PREFIX)) {
                    val messageId = key.removePrefix(MESSAGE_PREFIX)
                    val messageBytes = iterator.value()

                    if (messageBytes != null) {
                        try {
                            val messageJson = String(messageBytes)
                            val messageData = objectMapper.readTree(messageJson)

                            val msgTenantId = messageData.get("tenantId")?.asText()
                            val msgQueueName = messageData.get("queueName")?.asText()

                            // 큐 이름 매칭 (파티션 포함)
                            if (msgTenantId == tenantId &&
                                (msgQueueName == queueName || msgQueueName?.startsWith("${queueName}_partition_") == true)
                            ) {

                                val sentTimestampNode = messageData.get("sentTimestamp")
                                val sentTime = if (sentTimestampNode?.isTextual == true) {
                                    Instant.parse(sentTimestampNode.asText())
                                } else {
                                    Instant.ofEpochMilli((sentTimestampNode?.asDouble()?.times(1000))?.toLong() ?: 0L)
                                }

                                if (oldestTime == null || sentTime.isBefore(oldestTime)) {
                                    oldestTime = sentTime
                                }
                            }
                        } catch (e: Exception) {
                            logger.warn("메시지 파싱 중 오류: $messageId", e)
                        }
                    }
                }

                iterator.next()
            }

            iterator.close()
            oldestTime
        } catch (e: Exception) {
            logger.error("가장 오래된 메시지 시간 조회 실패: ${tenantId}:${queueName}", e)
            null
        }
    }
}


