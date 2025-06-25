package com.example.sqs.message.repository

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.message.domain.Message
import com.example.sqs.message.domain.MessagePreview
import com.example.sqs.queue.domain.Queue
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

@Repository
class RedisMessageRepository(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val redisKeyManager: RedisKeyManager
) : MessageRepository {

    private val logger = LoggerFactory.getLogger(RedisMessageRepository::class.java)

    override fun sendMessage(queue: Queue, message: Message): Mono<Message> {
        logger.info("메시지 전송 시작: messageId=${message.messageId}, queue=${queue.queueName}")
        
        return saveMessageDetails(message)
            .then(addToQueue(queue, message))
            .thenReturn(message)
            .doOnSuccess { logger.info("메시지 전송 완료: messageId=${message.messageId}") }
            .doOnError { error -> logger.error("메시지 전송 실패: messageId=${message.messageId}", error) }
    }

    override fun receiveMessages(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message> {
        logger.info("메시지 수신 시작: queue=${queue.queueName}, maxMessages=$maxMessages")
        
        // 먼저 만료된 메시지들을 복원
        return restoreExpiredMessages(queue)
            .then(moveDelayedToAvailable(queue))
            .thenMany(pollMessagesFromQueue(queue, maxMessages))
            .flatMap { messageId ->
                getMessageDetails(messageId)
                    .flatMap { message ->
                        val timeout = visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds
                        val receiptHandle = UUID.randomUUID().toString()
                        val updatedMessage = message.withNewReceiptHandle(receiptHandle, timeout)
                            .withIncrementedReceiveCount()
                        
                        saveMessageDetails(updatedMessage)
                            .then(addToInFlight(queue, updatedMessage))
                            .thenReturn(updatedMessage)
                    }
            }
            .doOnNext { logger.info("메시지 수신 완료: messageId=${it.messageId}") }
            .doOnError { error -> logger.error("메시지 수신 실패", error) }
    }

    override fun deleteMessage(queue: Queue, receiptHandle: String): Mono<Void> {
        logger.info("메시지 삭제 시작: receiptHandle=$receiptHandle")
        
        return removeFromInFlight(queue, receiptHandle)
            .doOnSuccess { logger.info("메시지 삭제 완료: receiptHandle=$receiptHandle") }
            .doOnError { error -> logger.error("메시지 삭제 실패: receiptHandle=$receiptHandle", error) }
    }

    override fun findByMessageId(messageId: String): Mono<Message> {
        return getMessageDetails(messageId)
    }

    override fun purgeAllMessages(queue: Queue): Mono<Void> {
        logger.info("큐 메시지 전체 삭제 시작: queue=${queue.queueName}")
        
        return redisTemplate.delete(
            redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName),
            redisKeyManager.getDelayedQueueKey(queue.tenantId, queue.queueName),
            redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName)
        ).then()
            .doOnSuccess { logger.info("큐 메시지 전체 삭제 완료: queue=${queue.queueName}") }
    }

    override fun restoreExpiredMessages(queue: Queue): Mono<Long> {
        val now = Instant.now().epochSecond
        
        return redisTemplate.opsForZSet()
            .removeRangeByScore(redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName), Range.closed(0.0, now.toDouble()))
            .map { count ->
                if (count > 0) {
                    logger.info("만료된 메시지 $count 개를 복원했습니다: queue=${queue.queueName}")
                }
                count
            }
    }

    override fun moveDelayedToAvailable(queue: Queue): Mono<Long> {
        val now = Instant.now().epochSecond
        
        return redisTemplate.opsForZSet()
            .rangeByScore(redisKeyManager.getDelayedQueueKey(queue.tenantId, queue.queueName), Range.closed(0.0, now.toDouble()))
            .collectList()
            .flatMap { messageIds ->
                if (messageIds.isNotEmpty()) {
                    logger.info("지연된 메시지 ${messageIds.size}개를 available로 이동: queue=${queue.queueName}")
                    redisTemplate.opsForZSet()
                        .removeRangeByScore(redisKeyManager.getDelayedQueueKey(queue.tenantId, queue.queueName), Range.closed(0.0, now.toDouble()))
                        .then(
                            redisTemplate.opsForList()
                                .rightPushAll(redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName), messageIds)
                        )
                        .map { messageIds.size.toLong() }
                } else {
                    Mono.just(0L)
                }
            }
    }

    private fun saveMessageDetails(message: Message): Mono<Void> {
        val messageData = objectMapper.writeValueAsString(message)
        return redisTemplate.opsForValue()
            .set(redisKeyManager.getMessageKey(message.messageId), messageData)
            .then()
    }

    private fun getMessageDetails(messageId: String): Mono<Message> {
        return redisTemplate.opsForValue()
            .get(redisKeyManager.getMessageKey(messageId))
            .map { json -> objectMapper.readValue(json, Message::class.java) }
    }

    private fun addToQueue(queue: Queue, message: Message): Mono<Long> {
        return if (message.visibleAt > Instant.now()) {
            // 지연된 메시지는 delayed queue에 추가
            val score = message.visibleAt.epochSecond.toDouble()
            redisTemplate.opsForZSet()
                .add(redisKeyManager.getDelayedQueueKey(queue.tenantId, queue.queueName), message.messageId, score)
                .map { if (it) 1L else 0L }
        } else {
            // 즉시 처리 가능한 메시지는 available queue에 추가
            redisTemplate.opsForList()
                .rightPush(redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName), message.messageId)
        }
    }

    private fun pollMessagesFromQueue(queue: Queue, maxMessages: Int): Flux<String> {
        return Flux.range(1, maxMessages)
            .flatMap {
                redisTemplate.opsForList()
                    .leftPop(redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName))
            }
            .filter { it != null }
    }

    private fun addToInFlight(queue: Queue, message: Message): Mono<Long> {
        val score = message.visibleAt.epochSecond.toDouble()
        val value = "${message.messageId}:${message.receiptHandle}"
        return redisTemplate.opsForZSet()
            .add(redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName), value, score)
            .map { if (it) 1L else 0L }
    }

    private fun removeFromInFlight(queue: Queue, receiptHandle: String): Mono<Void> {
        return redisTemplate.opsForZSet()
            .range(redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName), Range.closed(0L, -1L))
            .filter { it.contains(":$receiptHandle") }
            .flatMap { value ->
                redisTemplate.opsForZSet()
                    .remove(redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName), value)
            }
            .then()
    }

    override fun peekMessages(queue: Queue, maxMessages: Int): Flux<MessagePreview> {
        logger.info("메시지 미리보기 시작: queue=${queue.queueName}, maxMessages=$maxMessages")
        
        // available 큐에서 메시지 ID들을 가져와서 (제거하지 않고) 내용을 조회
        return redisTemplate.opsForList()
            .range(redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName), 0, (maxMessages - 1).toLong())
            .flatMap { messageId ->
                getMessageDetails(messageId)
                    .map { message -> MessagePreview.fromMessage(message) }
                    .onErrorContinue { error, _ ->
                        logger.warn("메시지 미리보기 중 오류 발생: messageId=$messageId", error)
                    }
            }
            .doOnComplete { logger.info("메시지 미리보기 완료: queue=${queue.queueName}") }
    }
}
