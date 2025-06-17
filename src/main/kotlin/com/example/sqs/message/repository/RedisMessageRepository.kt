package com.example.sqs.message.repository

import com.example.sqs.message.domain.Message
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
    private val objectMapper: ObjectMapper
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
            queue.getMessagesKey(),
            queue.getDelayedKey(),
            queue.getInFlightKey()
        ).then()
            .doOnSuccess { logger.info("큐 메시지 전체 삭제 완료: queue=${queue.queueName}") }
    }

    override fun restoreExpiredMessages(queue: Queue): Mono<Long> {
        val now = Instant.now().epochSecond
        
        return redisTemplate.opsForZSet()
            .removeRangeByScore(queue.getInFlightKey(), Range.closed(0.0, now.toDouble()))
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
            .rangeByScore(queue.getDelayedKey(), Range.closed(0.0, now.toDouble()))
            .collectList()
            .flatMap { messageIds ->
                if (messageIds.isNotEmpty()) {
                    logger.info("지연된 메시지 ${messageIds.size}개를 available로 이동: queue=${queue.queueName}")
                    redisTemplate.opsForZSet()
                        .removeRangeByScore(queue.getDelayedKey(), Range.closed(0.0, now.toDouble()))
                        .then(
                            redisTemplate.opsForList()
                                .rightPushAll(queue.getMessagesKey(), messageIds)
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
            .set(message.getMessageKey(), messageData)
            .then()
    }

    private fun getMessageDetails(messageId: String): Mono<Message> {
        return redisTemplate.opsForValue()
            .get("message:$messageId")
            .map { json -> objectMapper.readValue(json, Message::class.java) }
    }

    private fun addToQueue(queue: Queue, message: Message): Mono<Long> {
        return if (message.visibleAt > Instant.now()) {
            // 지연된 메시지는 delayed queue에 추가
            val score = message.visibleAt.epochSecond.toDouble()
            redisTemplate.opsForZSet()
                .add(queue.getDelayedKey(), message.messageId, score)
                .map { if (it) 1L else 0L }
        } else {
            // 즉시 처리 가능한 메시지는 available queue에 추가
            redisTemplate.opsForList()
                .rightPush(queue.getMessagesKey(), message.messageId)
        }
    }

    private fun pollMessagesFromQueue(queue: Queue, maxMessages: Int): Flux<String> {
        return Flux.range(1, maxMessages)
            .flatMap {
                redisTemplate.opsForList()
                    .leftPop(queue.getMessagesKey())
            }
            .filter { it != null }
    }

    private fun addToInFlight(queue: Queue, message: Message): Mono<Long> {
        val score = message.visibleAt.epochSecond.toDouble()
        val value = "${message.messageId}:${message.receiptHandle}"
        return redisTemplate.opsForZSet()
            .add(queue.getInFlightKey(), value, score)
            .map { if (it) 1L else 0L }
    }

    private fun removeFromInFlight(queue: Queue, receiptHandle: String): Mono<Void> {
        return redisTemplate.opsForZSet()
            .range(queue.getInFlightKey(), Range.closed(0L, -1L))
            .filter { it.contains(":$receiptHandle") }
            .flatMap { value ->
                redisTemplate.opsForZSet()
                    .remove(queue.getInFlightKey(), value)
            }
            .then()
    }
}
