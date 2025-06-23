package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class FifoStorageService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    @Value("\${queue.partition-count:4}") private val partitionCount: Int
) {

    private val logger = LoggerFactory.getLogger(FifoStorageService::class.java)

    /**
     * FIFO 큐에 메시지를 저장 (Hot Storage에 직접 저장, 파티션 기반)
     */
    fun sendMessageToHot(queue: Queue, message: Message): Mono<Message> {
        val partition = determinePartition(message)

        return saveMessageDetails(message)
            .then(addToHotStorage(queue, message, partition))
            .thenReturn(message)
            .doOnSuccess {
                logger.debug("FIFO 메시지를 Hot Storage 파티션 $partition 에 저장: messageId=${message.messageId}")
            }
    }

    /**
     * Message Group ID를 기반으로 파티션 결정 (FIFO에서 MessageGroupId는 필수)
     */
    private fun determinePartition(message: Message): Int {
        val messageGroupId = message.messageGroupId
            ?: message.attributes["MessageGroupId"]
            ?: throw IllegalStateException("FIFO 큐에서 MessageGroupId가 누락되었습니다")
        return Math.abs(messageGroupId.hashCode()) % partitionCount
    }

    /**
     * Hot Storage에 메시지 추가 (파티션 기반)
     */
    private fun addToHotStorage(queue: Queue, message: Message, partition: Int): Mono<Void> {
        val hotKey = queue.getHotKey(partition)

        return if (message.delaySeconds > 0) {
            // 지연 메시지는 별도 처리 (파티션 기반)
            addToDelayedQueue(queue, message, partition)
        } else {
            // 즉시 처리 메시지는 Hot Storage에 추가
            redisTemplate.opsForList()
                .rightPush(hotKey, message.messageId)
                .then()
                .then(updateMessageTier(message.messageId, "HOT"))
        }
    }

    /**
     * 지연 메시지를 지연 큐에 추가 (파티션 기반)
     */
    private fun addToDelayedQueue(queue: Queue, message: Message, partition: Int): Mono<Void> {
        val delayedKey = queue.getDelayedKey(partition)
        val executeAt = message.visibleAt.epochSecond

        return redisTemplate.opsForZSet()
            .add(delayedKey, message.messageId, executeAt.toDouble())
            .then()
            .then(updateMessageTier(message.messageId, "DELAYED"))
    }

    /**
     * 메시지 상세 정보 저장 (JSON 형식으로 통일)
     */
    private fun saveMessageDetails(message: Message): Mono<Void> {
        val messageKey = "message:${message.messageId}"
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
            .put("message:$messageId", "tier", tier)
            .then()
    }

}

/**
 * Queue 확장 함수들 (파티션 지원)
 */
private fun Queue.getHotKey(partition: Int): String = "hot:queue:$tenantId:${queueName}_partition_$partition"
private fun Queue.getDelayedKey(partition: Int): String = "delayed:queue:$tenantId:${queueName}_partition_$partition"
