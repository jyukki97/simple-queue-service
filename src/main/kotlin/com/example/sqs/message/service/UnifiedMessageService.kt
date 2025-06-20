package com.example.sqs.message.service

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.util.MessageStorageUtils
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

@Service
class UnifiedMessageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val messageStorageUtils: MessageStorageUtils,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int
) {

    private val logger = LoggerFactory.getLogger(UnifiedMessageService::class.java)

    /**
     * 통합된 메시지 수신 (파티션 기반, Hot → Warm → Cold 순서)
     */
    fun receiveMessages(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message> {
        logger.debug("메시지 수신 시작: queue=${queue.queueName}, maxMessages=$maxMessages, fifo=${queue.attributes.fifoQueue}")

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
                logger.debug("메시지 수신 완료: messageId=${message.messageId}")
            }
    }

    /**
     * 통합된 메시지 전송 (파티션 기반)
     */
    fun sendMessageToHot(queue: Queue, message: Message): Mono<Message> {
        val partition = determinePartition(queue, message)

        return messageStorageUtils.saveMessageDetails(message)
            .then(addToHotStorage(queue, message, partition))
            .thenReturn(message)
            .doOnSuccess {
                logger.debug("메시지를 Hot Storage 파티션 $partition 에 저장: messageId=${message.messageId}")
            }
    }

    /**
     * 모든 파티션에서 메시지 수신 시도
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
     * Hot Storage에서 메시지 수신
     */
    private fun receiveFromHotPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val hotKey = queue.getHotKey(partition)

        return if (queue.attributes.fifoQueue == true) {
            // FIFO: 순서 보장을 위해 LPOP 사용
            redisTemplate.opsForList()
                .leftPop(hotKey)
                .repeat((maxMessages - 1).toLong())
                .filter { it != null }
        } else {
            // Standard: RPOP으로 LIFO 방식
            redisTemplate.opsForList()
                .rightPop(hotKey)
                .repeat((maxMessages - 1).toLong())
                .filter { it != null }
        }
            .flatMap { messageId -> messageStorageUtils.getMessageDetails(messageId) }
    }

    /**
     * Warm Storage에서 메시지 수신 (자동 Hot 승격)
     */
    private fun receiveFromWarmPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val warmKey = queue.getWarmKey(partition)

        return redisTemplate.opsForZSet()
            .range(warmKey, Range.closed(0L, (maxMessages - 1).toLong()))
            .flatMap { messageId ->
                promoteToHot(queue, messageId, partition)
                    .then(messageStorageUtils.getMessageDetails(messageId))
            }
    }

    /**
     * Cold Storage에서 메시지 수신 (자동 Hot 승격)
     */
    private fun receiveFromColdPartition(queue: Queue, partition: Int, maxMessages: Int): Flux<Message> {
        val coldKey = queue.getColdKey(partition)

        return redisTemplate.opsForZSet()
            .range(coldKey, Range.closed(0L, (maxMessages - 1).toLong()))
            .flatMap { messageId ->
                promoteToHot(queue, messageId, partition)
                    .then(messageStorageUtils.getMessageDetails(messageId))
            }
    }

    /**
     * 메시지를 Hot Storage로 승격
     */
    private fun promoteToHot(queue: Queue, messageId: String, partition: Int): Mono<Void> {
        val hotKey = queue.getHotKey(partition)
        val warmKey = queue.getWarmKey(partition)
        val coldKey = queue.getColdKey(partition)

        return redisTemplate.opsForZSet().remove(warmKey, messageId)
            .then(redisTemplate.opsForZSet().remove(coldKey, messageId))
            .then(redisTemplate.opsForList().rightPush(hotKey, messageId))
            .then()
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
     * InFlight 큐에 메시지 추가
     */
    private fun addToInFlight(queue: Queue, message: Message, timeoutSeconds: Int): Mono<Void> {
        val partition = determinePartition(queue, message)
        val inFlightKey = queue.getInFlightKey(partition)
        val score = message.visibleAt.epochSecond.toDouble()
        val value = "${message.messageId}:${message.receiptHandle}"

        return redisTemplate.opsForZSet()
            .add(inFlightKey, value, score)
            .then()
    }

    /**
     * Hot Storage에 메시지 추가
     */
    private fun addToHotStorage(queue: Queue, message: Message, partition: Int): Mono<Void> {
        val hotKey = queue.getHotKey(partition)

        return if (message.delaySeconds > 0) {
            // 지연 메시지는 delayed 큐에 저장
            val delayedKey = queue.getDelayedKey(partition)
            val score = message.visibleAt.epochSecond.toDouble()
            redisTemplate.opsForZSet().add(delayedKey, message.messageId, score).then()
        } else {
            // 즉시 처리 가능한 메시지는 Hot Storage에 저장
            redisTemplate.opsForList().rightPush(hotKey, message.messageId).then()
        }
    }

    /**
     * 파티션 결정 로직 (통합)
     */
    private fun determinePartition(queue: Queue, message: Message): Int {
        return if (queue.attributes.fifoQueue == true) {
            // FIFO: MessageGroupId 기반 (순서 보장)
            val messageGroupId = message.attributes["MessageGroupId"] ?: message.messageId
            Math.abs(messageGroupId.hashCode()) % partitionCount
        } else {
            // Standard: MessageGroupId가 있으면 사용, 없으면 messageId 사용
            val groupId = message.attributes["MessageGroupId"] ?: message.messageId
            Math.abs(groupId.hashCode()) % partitionCount
        }
    }
}

/**
 * Queue 확장 함수들 (파티션 지원)
 */
private fun Queue.getHotKey(partition: Int): String = "hot:queue:$tenantId:${queueName}_partition_$partition"
private fun Queue.getWarmKey(partition: Int): String = "warm:queue:$tenantId:${queueName}_partition_$partition"
private fun Queue.getColdKey(partition: Int): String = "cold:queue:$tenantId:${queueName}_partition_$partition"
private fun Queue.getDelayedKey(partition: Int): String = "delayed:queue:$tenantId:${queueName}_partition_$partition"
private fun Queue.getInFlightKey(partition: Int): String = "inflight:queue:$tenantId:${queueName}_partition_$partition"
