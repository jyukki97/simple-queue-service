package com.example.sqs.storage.service

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.message.domain.Message
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.queue.domain.Queue
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

@Service
class FifoMessageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val messageRepository: MessageRepository,
    private val objectMapper: ObjectMapper,
    private val redisKeyManager: RedisKeyManager,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int
) {

    private val logger = LoggerFactory.getLogger(FifoMessageService::class.java)

    /**
     * FIFO 큐의 최적화된 메시지 수신 (파티션 기반)
     * Hot → Warm → Cold 순서로 검색하되, Hot에서 대부분 처리
     */
    fun receiveMessagesFifo(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message> {
        logger.debug("FIFO 메시지 수신 시작: queue=${queue.queueName}, maxMessages=$maxMessages")

        return receiveFromAllPartitions(queue, maxMessages, "hot")
            .switchIfEmpty(
                receiveFromAllPartitions(queue, maxMessages, "warm")
                    .switchIfEmpty(
                        receiveFromAllPartitions(queue, maxMessages, "cold")
                    )
            )
            .take(maxMessages.toLong())
            .flatMap { message ->
                processReceivedMessage(queue, message, visibilityTimeoutSeconds)
            }
            .doOnNext { message ->
                logger.debug("FIFO 메시지 수신 완료: messageId=${message.messageId}")
            }
    }

    /**
     * 모든 파티션에서 메시지 수신
     */
    private fun receiveFromAllPartitions(queue: Queue, maxMessages: Int, tier: String): Flux<Message> {
        return Flux.range(0, partitionCount)
            .flatMap { partition ->
                when (tier) {
                    "hot" -> receiveFromHotPartition(queue, partition, maxMessages)
                    "warm" -> receiveFromWarmPartition(queue, partition, maxMessages)
                    "cold" -> receiveFromColdPartition(queue, partition, maxMessages)
                    else -> Flux.empty()
                }
            }
    }

    /**
     * Hot Storage 파티션에서 메시지 수신
     */
    private fun receiveFromHotPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val hotKey = redisKeyManager.getHotQueueKey(queue, partition)

        return redisTemplate.opsForList()
            .leftPop(hotKey, maxMessages.toLong())
            .flatMap { messageId ->
                getMessageDetails(messageId)
                    .doOnSuccess {
                        logger.info("HOT에서 메시지 수신: queueName=${queue.queueName}, partition=$partition, messageId=$messageId")
                    }
            }
    }

    /**
     * Warm Storage 파티션에서 메시지 수신 (자동으로 Hot으로 승격)
     */
    private fun receiveFromWarmPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val warmKey = redisKeyManager.getWarmQueueKey(queue.tenantId, queue.queueName, partition)

        return redisTemplate.opsForList()
            .leftPop(warmKey, maxMessages.toLong())
            .flatMap { messageId ->
                getMessageDetails(messageId)
                    .flatMap { message ->
                        // Warm에서 가져온 메시지는 Hot으로 승격
                        promoteToHot(queue, messageId, partition)
                            .thenReturn(message)
                            .doOnSuccess {
                                logger.info("WARM에서 메시지 수신 및 HOT 승격: queueName=${queue.queueName}, partition=$partition, messageId=$messageId")
                            }
                    }
            }
    }

    /**
     * Cold Storage 파티션에서 메시지 수신 (자동으로 Hot으로 승격)
     */
    private fun receiveFromColdPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val coldKey = redisKeyManager.getColdQueueKey(queue.tenantId, queue.queueName)

        return redisTemplate.opsForList()
            .leftPop(coldKey, maxMessages.toLong())
            .flatMap { messageId ->
                getMessageDetails(messageId)
                    .flatMap { message ->
                        // Cold에서 가져온 메시지는 Hot으로 승격
                        promoteToHot(queue, messageId, partition)
                            .thenReturn(message)
                            .doOnSuccess {
                                logger.info("COLD에서 메시지 수신 및 HOT 승격: queueName=${queue.queueName}, partition=$partition, messageId=$messageId")
                            }
                    }
            }
    }

    /**
     * 메시지를 Hot Storage 파티션으로 승격
     */
    private fun promoteToHot(queue: Queue, messageId: String, partition: Int): Mono<Void> {
        val hotKey = redisKeyManager.getHotQueueKey(queue, partition)

        return redisTemplate.opsForList()
            .leftPush(hotKey, messageId)
            .then()
            .then(updateMessageTier(messageId, "HOT"))
    }

    /**
     * 수신된 메시지 처리 (가시성 타임아웃 설정, InFlight 큐 추가)
     */
    private fun processReceivedMessage(queue: Queue, message: Message, visibilityTimeoutSeconds: Int?): Mono<Message> {
        val timeout = visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds
        val receiptHandle = "${message.messageId}:${UUID.randomUUID()}"

        val updatedMessage = message.copy(
            receiptHandle = receiptHandle,
            receiveCount = message.receiveCount + 1,
            firstReceiveTimestamp = message.firstReceiveTimestamp ?: Instant.now()
        )

        return saveMessageDetails(updatedMessage)
            .then(addToInFlight(queue, updatedMessage, timeout))
            .thenReturn(updatedMessage)
    }

    /**
     * 메시지를 InFlight 큐에 추가 (파티션 고려)
     */
    private fun addToInFlight(queue: Queue, message: Message, timeoutSeconds: Int): Mono<Void> {
        val inFlightKey = redisKeyManager.getInFlightQueueKey(queue.tenantId, queue.queueName)
        val visibilityTimeout = Instant.now().plusSeconds(timeoutSeconds.toLong()).epochSecond

        return redisTemplate.opsForZSet()
            .add(inFlightKey, message.receiptHandle!!, visibilityTimeout.toDouble())
            .then()
    }

    /**
     * 메시지 상세 정보 조회 (JSON 형식으로 통일)
     */
    private fun getMessageDetails(messageId: String): Mono<Message> {
        return redisTemplate.opsForValue()
            .get(redisKeyManager.getMessageKey(messageId))
            .map { messageJson ->
                objectMapper.readValue(messageJson, Message::class.java)
            }
    }

    /**
     * 메시지 상세 정보 저장 (JSON 형식으로 통일)
     */
    private fun saveMessageDetails(message: Message): Mono<Void> {
        val messageKey = redisKeyManager.getMessageKey(message.messageId)
        val messageJson = objectMapper.writeValueAsString(message)

        return redisTemplate.opsForValue()
            .set(messageKey, messageJson)
            .then()
    }

    /**
     * 메시지의 현재 계층 정보 업데이트
     */
    private fun updateMessageTier(messageId: String, tier: String): Mono<Void> {
        return redisTemplate.opsForHash<String, String>()
            .put(redisKeyManager.getMessageKey(messageId), "tier", tier)
            .then()
    }

    /**
     * Standard 큐의 기본 메시지 수신 (기존 로직 유지)
     */
    fun receiveMessagesStandard(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message> {
        // 기존 MessageRepository의 receiveMessages 호출
        return messageRepository.receiveMessages(queue, maxMessages, visibilityTimeoutSeconds)
    }

    /**
     * 큐 타입에 따른 적절한 수신 방법 선택
     */
    fun receiveMessages(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message> {
        return if (queue.attributes.fifoQueue == true) {
            receiveMessagesFifo(queue, maxMessages, visibilityTimeoutSeconds)
        } else {
            receiveMessagesStandard(queue, maxMessages, visibilityTimeoutSeconds)
        }
    }

}