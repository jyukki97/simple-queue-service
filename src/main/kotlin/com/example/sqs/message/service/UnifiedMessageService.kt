package com.example.sqs.message.service

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.service.StorageStatsService
import com.example.sqs.storage.service.WarmTierManagerService
import com.example.sqs.storage.util.MessageStorageUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*
import kotlin.math.abs

@Service
class UnifiedMessageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val messageStorageUtils: MessageStorageUtils,
    private val warmTierManagerService: WarmTierManagerService,
    private val storageStatsService: StorageStatsService,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int,
    private val redisKeyManager: RedisKeyManager
) {

    private val logger = LoggerFactory.getLogger(UnifiedMessageService::class.java)

    /**
     * 통합된 메시지 수신 (올바른 아키텍처: Available 큐 → Location 확인 → 실제 스토리지 조회)
     */
    fun receiveMessages(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?, messageGroupId: String? = null, waitTimeSeconds: Int = 0): Flux<Message> {
        logger.debug("메시지 수신 시작: queue=${queue.queueName}, maxMessages=$maxMessages, fifo=${queue.attributes.fifoQueue}, waitTime=${waitTimeSeconds}s")

        return receiveMessagesInternal(queue, maxMessages, messageGroupId)
            .take(maxMessages.toLong())
            .collectList()
            .flatMapMany { messages ->
                if (messages.isNotEmpty() || waitTimeSeconds <= 0) {
                    // 메시지가 있거나 대기 시간이 없으면 즉시 반환
                    Flux.fromIterable(messages)
                } else {
                    // Long Polling: 메시지가 없으면 잠시 대기 후 재시도
                    performLongPolling(queue, maxMessages, waitTimeSeconds, messageGroupId)
                }
            }
            .flatMap { message -> processReceivedMessage(queue, message, visibilityTimeoutSeconds) }
            .doOnNext { message -> logger.debug("메시지 수신 완료: messageId=${message.messageId}") }
    }

    /**
     * 메시지 수신 핵심 로직 (공통)
     */
    private fun receiveMessagesInternal(queue: Queue, maxMessages: Int, messageGroupId: String?): Flux<Message> {
        return if (queue.attributes.fifoQueue && messageGroupId != null) {
            // FIFO 큐에서 특정 MessageGroupId 파티션에서만 수신
            val partition = abs(messageGroupId.hashCode()) % partitionCount
            receiveFromAvailableQueue(queue, partition, maxMessages)
                .filter { message ->
                    // 메시지의 MessageGroupId가 요청한 것과 일치하는지 확인
                    val msgGroupId = message.messageGroupId ?: message.attributes["MessageGroupId"]
                    msgGroupId == messageGroupId
                }
        } else {
            // Standard 큐: 모든 파티션에서 라운드로빈으로 수신
            receiveFromAllAvailableQueues(queue, maxMessages)
        }
    }

    /**
     * Long Polling 구현 (공통 로직 활용)
     */
    private fun performLongPolling(queue: Queue, maxMessages: Int, waitTimeSeconds: Int, messageGroupId: String?): Flux<Message> {
        val pollInterval = 500L // 500ms마다 폴링
        val maxPolls = (waitTimeSeconds * 1000L / pollInterval).toInt()

        return Flux.range(0, maxPolls)
            .concatMap { pollCount ->
                // 각 폴링 간격마다 대기
                if (pollCount > 0) {
                    Mono.delay(java.time.Duration.ofMillis(pollInterval))
                        .then(Mono.empty<Message>())
                } else {
                    Mono.empty()
                }
                    .thenMany(receiveMessagesInternal(queue, maxMessages, messageGroupId))
            }
            .take(maxMessages.toLong())
            .collectList()
            .flatMapMany { messages ->
                if (messages.isNotEmpty()) {
                    Flux.fromIterable(messages)
                } else {
                    Flux.empty()
                }
            }
    }

    /**
     * 통합된 메시지 전송 (파티션 기반)
     */
    fun sendMessageToHot(queue: Queue, message: Message): Mono<Message> {
        val partition = determinePartition(queue, message)

        return messageStorageUtils.saveMessageDetails(message)
            .then(addToHotStorage(queue, message, partition))
            .then(
                // HOT tier index에 메시지 추가
                storageStatsService.updateTierIndex(message.messageId, queue, null, "HOT")
                    .onErrorResume { 
                        logger.warn("Tier index 업데이트 실패: ${message.messageId}", it)
                        Mono.empty() 
                    }
            )
            .thenReturn(message)
            .doOnSuccess {
                logger.debug("메시지를 Hot Storage 파티션 $partition 에 저장: messageId=${message.messageId}")
            }
    }

    /**
     * 모든 Available 큐에서 라운드로빈으로 메시지 수신 (올바른 아키텍처)
     */
    private fun receiveFromAllAvailableQueues(queue: Queue, maxMessages: Int): Flux<Message> {
        return Flux.range(0, partitionCount)
            .concatMap { partition ->
                val messagesPerPartition = kotlin.math.ceil(maxMessages.toDouble() / partitionCount).toInt()
                receiveFromAvailableQueue(queue, partition, messagesPerPartition)
            }
            .take(maxMessages.toLong())
    }

    /**
     * 특정 파티션의 Available 큐에서 메시지 수신 (올바른 아키텍처)
     */
    private fun receiveFromAvailableQueue(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val availableKey = redisKeyManager.getAvailableQueueKey(queue, partition)

        return Flux.range(0, maxMessages)
            .concatMap {
                // 1. Available 큐에서 메시지 ID를 pop
                val popOperation = if (queue.attributes.fifoQueue) {
                    redisTemplate.opsForList().leftPop(availableKey) // FIFO: 순서 보장
                } else {
                    redisTemplate.opsForList().rightPop(availableKey) // Standard: LIFO
                }

                popOperation
                    .flatMap { messageId ->
                        // 2. 메시지 위치 정보 확인
                        getMessageLocation(messageId)
                            .flatMap { location ->
                                // 3. 위치에 따라 실제 메시지 데이터 조회
                                getMessageFromStorage(messageId, location)
                            }
                    }
            }
            .filter { it != null }
            .doOnNext { message ->
                logger.debug("Available 큐에서 메시지 수신: partition=$partition, messageId=${message.messageId}")
            }
    }

    /**
     * 메시지 위치 정보 조회 (message:location:{messageId})
     */
    private fun getMessageLocation(messageId: String): Mono<String> {
        val locationKey = "message:location:$messageId"
        return redisTemplate.opsForValue()
            .get(locationKey)
            .defaultIfEmpty("HOT") // 기본값은 HOT
            .doOnNext { location ->
                logger.debug("메시지 위치 확인: messageId=$messageId, location=$location")
            }
    }

    /**
     * 스토리지 위치에 따라 실제 메시지 데이터 조회
     */
    private fun getMessageFromStorage(messageId: String, location: String): Mono<Message> {
        return when (location.uppercase()) {
            "HOT" -> {
                // Redis에서 메시지 상세 정보 조회
                messageStorageUtils.getMessageDetails(messageId)
                    .doOnNext { logger.debug("HOT 스토리지에서 메시지 조회: messageId=$messageId") }
            }

            "WARM" -> {
                // RocksDB에서 메시지 조회 (승격 없이 그대로 조회)
                warmTierManagerService.getMessageFromWarm(messageId)
                    .cast(Message::class.java) // Mono<Message?> -> Mono<Message>
                    .doOnNext { logger.debug("WARM 스토리지에서 메시지 조회: messageId=$messageId") }
            }

            "COLD" -> {
                // 로컬 스토리지에서 메시지 조회 (승격 없이 그대로 조회)
                getMessageFromColdStorage(messageId)
                    .doOnNext { logger.debug("COLD 스토리지에서 메시지 조회: messageId=$messageId") }
            }

            else -> {
                logger.warn("알 수 없는 스토리지 위치: location=$location, messageId=$messageId")
                Mono.empty()
            }
        }
    }

    /**
     * COLD 스토리지에서 메시지 조회 (실제 구현 필요)
     */
    private fun getMessageFromColdStorage(messageId: String): Mono<Message> {
        // TODO: 실제 COLD 스토리지 구현체와 연동
        logger.warn("COLD 스토리지 조회 미구현: messageId=$messageId")
        return Mono.empty()
    }

    /**
     * 수신된 메시지 처리 (Visibility Timeout 설정)
     */
    private fun processReceivedMessage(queue: Queue, message: Message, visibilityTimeoutSeconds: Int?): Mono<Message> {
        val timeout = visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds
        val receiptHandle = UUID.randomUUID().toString()
        val visibleAt = Instant.now().plusSeconds(timeout.toLong())

        val updatedMessage = message.copy(
            receiptHandle = receiptHandle,
            receiveCount = message.receiveCount + 1,
            visibleAt = visibleAt
        )

        return messageStorageUtils.saveMessageDetails(updatedMessage)
            .then(addToInFlight(queue, updatedMessage, timeout))
            .thenReturn(updatedMessage)
    }

    /**
     * InFlight 큐에 메시지 추가 (ZSet 타입 - 시간 기반 만료 관리)
     */
    private fun addToInFlight(queue: Queue, message: Message, timeoutSeconds: Int): Mono<Void> {
        val partition = determinePartition(queue, message)
        val inFlightKey = redisKeyManager.getProcessingQueueKey(queue, partition)
        val score = message.visibleAt.epochSecond.toDouble()
        val value = "${message.messageId}:${message.receiptHandle}"

        return redisTemplate.opsForZSet()
            .add(inFlightKey, value, score)
            .then()
    }

    /**
     * Hot Storage에 메시지 추가 (List 타입으로 통일)
     */
    private fun addToHotStorage(queue: Queue, message: Message, partition: Int): Mono<Void> {
        val availableKey = redisKeyManager.getAvailableQueueKey(queue, partition)

        return if (message.delaySeconds > 0) {
            // 지연 메시지는 delayed 큐에 저장 (ZSet 타입 - 시간 기반 정렬)
            val delayedKey = redisKeyManager.getDelayedQueueKey(queue, partition)
            val score = message.visibleAt.epochSecond.toDouble()
            redisTemplate.opsForZSet().add(delayedKey, message.messageId, score).then()
        } else {
            // 즉시 처리 가능한 메시지는 Available 큐에 저장 (List 타입)
            redisTemplate.opsForList().rightPush(availableKey, message.messageId).then()
        }
    }

    /**
     * 파티션 결정 로직 (통합)
     */
    private fun determinePartition(queue: Queue, message: Message): Int {
        return if (queue.attributes.fifoQueue) {
            // FIFO: MessageGroupId 필수 (Message 객체에서 직접 가져오거나 attributes에서)
            val messageGroupId = message.messageGroupId
                ?: message.attributes["MessageGroupId"]
                ?: throw IllegalStateException("FIFO 큐에서 MessageGroupId가 누락되었습니다")
            abs(messageGroupId.hashCode()) % partitionCount
        } else {
            // Standard: MessageGroupId가 있으면 사용, 없으면 messageId 사용
            val groupId = message.messageGroupId
                ?: message.attributes["MessageGroupId"]
                ?: message.messageId
            abs(groupId.hashCode()) % partitionCount
        }
    }
}
