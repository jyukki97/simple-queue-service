package com.example.sqs.message.service

import com.example.sqs.common.util.MessageParsingUtils
import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.service.MessageTierService
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

/**
 * 계층 구조를 고려한 메시지 수신 서비스
 */
@Service
class TieredMessageReceiveService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val messageTierService: MessageTierService
) {

    private val logger = LoggerFactory.getLogger(TieredMessageReceiveService::class.java)

    /**
     * 계층 우선순위에 따라 메시지 수신
     * 1. HOT 티어에서 먼저 수신 시도
     * 2. HOT이 비어있으면 WARM 티어에서 수신
     * 3. WARM도 비어있으면 COLD 티어에서 수신
     */
    fun receiveMessagesFromTiers(
        queue: Queue,
        maxMessages: Int,
        visibilityTimeoutSeconds: Int?
    ): Flux<Message> {

        logger.debug("계층별 메시지 수신 시작 - 큐: ${queue.queueName}, 최대: $maxMessages")

        return receiveFromTier(queue, "HOT", maxMessages)
            .switchIfEmpty(receiveFromTier(queue, "WARM", maxMessages))
            .switchIfEmpty(receiveFromTier(queue, "COLD", maxMessages))
            .take(maxMessages.toLong())
            .flatMap { messageId ->
                convertToMessage(messageId, queue, visibilityTimeoutSeconds)
            }
            .doOnNext { message ->
                logger.debug("메시지 수신 완료: ${message.messageId} from tier")
            }
    }

    /**
     * 특정 티어에서 메시지 수신
     */
    private fun receiveFromTier(queue: Queue, tier: String, maxMessages: Int): Flux<String> {
        return messageTierService.getMessagesInTier(queue, tier, maxMessages, 0)
            .take(maxMessages.toLong())
            .flatMap { messageId ->
                // 메시지를 해당 티어에서 제거 (LPOP 방식)
                removeMessageFromTier(queue, tier, messageId)
                    .map { messageId }
            }
    }

    /**
     * 티어에서 메시지 제거
     */
    private fun removeMessageFromTier(queue: Queue, tier: String, messageId: String): Mono<Boolean> {
        return if (queue.attributes.fifoQueue == true) {
            val queueKey = "${tier.lowercase()}:queue:${queue.tenantId}:${queue.queueName}"
            redisTemplate.opsForList().remove(queueKey, 1, messageId)
                .map { it > 0 }
        } else {
            // Standard 큐는 파티션별로 제거
            val partitionCount = 4
            Flux.range(0, partitionCount)
                .flatMap { partitionNumber ->
                    val partitionedQueueName = queue.queueName + "_partition_" + partitionNumber
                    val queueKey = "${tier.lowercase()}:queue:${queue.tenantId}:${partitionedQueueName}"
                    redisTemplate.opsForList().remove(queueKey, 1, messageId)
                }
                .any { it > 0 }
        }
    }

    /**
     * 메시지 ID를 Message 객체로 변환
     */
    private fun convertToMessage(
        messageId: String,
        queue: Queue,
        visibilityTimeoutSeconds: Int?
    ): Mono<Message> {
        return redisTemplate.opsForValue()
            .get("message:$messageId")
            .map { messageJson ->
                val messageData = MessageParsingUtils.parseMessageData(messageJson)
                if (messageData != null) {
                    createMessageFromData(messageId, messageData, queue, visibilityTimeoutSeconds)
                } else {
                    createErrorMessage(messageId, queue)
                }
            }
            .switchIfEmpty(
                Mono.just(createErrorMessage(messageId, queue))
            )
    }

    /**
     * 메시지 데이터에서 Message 객체 생성
     */
    private fun createMessageFromData(
        messageId: String,
        messageData: Map<String, Any>,
        queue: Queue,
        visibilityTimeoutSeconds: Int?
    ): Message {
        val body = messageData["messageBody"]?.toString() ?: ""
        val attributes = MessageParsingUtils.extractAttributes(messageData.toString())
        val sentTimestamp = MessageParsingUtils.parseTimestamp(messageData, "sentTimestamp")
        val receiveCount = (messageData["receiveCount"] as? Number)?.toInt() ?: 0
        val receiptHandle = generateReceiptHandle()

        val visibilityTimeout = visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds
        val visibleAt = Instant.now().plusSeconds(visibilityTimeout.toLong())

        return Message(
            messageId = messageId,
            tenantId = queue.tenantId,
            queueName = queue.queueName,
            messageBody = body,
            attributes = attributes,
            sentTimestamp = sentTimestamp,
            firstReceiveTimestamp = Instant.now(),
            receiveCount = receiveCount + 1,
            visibleAt = visibleAt,
            receiptHandle = receiptHandle,
            delaySeconds = 0
        )
    }

    /**
     * 에러 메시지 생성
     */
    private fun createErrorMessage(messageId: String, queue: Queue): Message {
        return Message(
            messageId = messageId,
            tenantId = queue.tenantId,
            queueName = queue.queueName,
            messageBody = "메시지 로드 실패",
            attributes = emptyMap(),
            sentTimestamp = Instant.now(),
            firstReceiveTimestamp = Instant.now(),
            receiveCount = 1,
            visibleAt = Instant.now().plusSeconds(30),
            receiptHandle = generateReceiptHandle(),
            delaySeconds = 0
        )
    }

    /**
     * Receipt Handle 생성
     */
    private fun generateReceiptHandle(): String {
        return UUID.randomUUID().toString()
    }
}
