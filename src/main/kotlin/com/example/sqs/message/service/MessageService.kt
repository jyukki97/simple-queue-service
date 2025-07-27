package com.example.sqs.message.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.config.QueueProperties
import com.example.sqs.message.domain.*
import com.example.sqs.message.factory.MessageFactory
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.metrics.service.MetricsService
import com.example.sqs.performance.monitoring.PerformanceMonitoringService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.service.QueueService
import com.example.sqs.streaming.service.StreamingService
import com.example.sqs.tracing.service.MessageTraceService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import kotlin.random.Random

/**
 * 메시지 관련 핵심 비즈니스 로직을 담당하는 서비스
 * 리팩토링: 책임 분리를 통해 각각의 전문 서비스로 위임
 */
@Service
class MessageService(
    private val messageRepository: MessageRepository,
    private val queueService: QueueService,
    private val messageTraceService: MessageTraceService,
    private val metricsService: MetricsService,
    private val performanceMonitoringService: PerformanceMonitoringService,
    private val unifiedMessageService: UnifiedMessageService,
    private val queueProperties: QueueProperties,
    private val messageValidationService: MessageValidationService,
    private val messageProcessingService: MessageProcessingService,
    private val messageDeletionService: MessageDeletionService,
    private val messageBatchService: MessageBatchService,
    private val messageFactory: MessageFactory,
    private val streamingService: StreamingService
) : BaseReactiveService() {

    override fun getServiceName(): String = "MessageService"

    /**
     * 큐에 메시지를 전송합니다 (성능 최적화 적용)
     */
    fun sendMessage(tenantId: String, queueName: String, request: SendMessageRequest): Mono<SendMessageResponse> {
        val traceId = messageTraceService.generateTraceId()

        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                // FIFO 큐 검증 (위임)
                if (queue.attributes.fifoQueue) {
                    messageValidationService.validateFifoMessage(request, queue)
                }

                // 메시지 압축 처리 (위임)
                val (messageBody, compressed) = messageProcessingService.processMessageCompression(request.messageBody)

                // 파티션 처리 (위임)
                val targetQueueName = calculateTargetQueueName(queueName, request, queue)

                // 메시지 생성 및 전송
                val message = messageFactory.createMessage(
                    tenantId = tenantId,
                    targetQueueName = targetQueueName,
                    messageBody = messageBody,
                    attributes = request.attributes,
                    delaySeconds = request.delaySeconds,
                    messageGroupId = request.messageGroupId,
                    messageDeduplicationId = request.messageDeduplicationId,
                    compressed = compressed,
                    originalMessageBodyLength = if (compressed) request.messageBody.length else null
                )

                sendMessageInternal(queue, message, traceId, tenantId, queueName, request, compressed)
            }
            .doOnSuccess {
                performanceMonitoringService.incrementCounter("sendMessage", tenantId, queueName)
            }
            .onErrorResume { error ->
                performanceMonitoringService.incrementErrorCounter(
                    "sendMessage", tenantId, error.javaClass.simpleName, queueName
                )
                handleError<SendMessageResponse>("sendMessage")(error)
            }
    }

    /**
     * 큐에 메시지를 일괄 전송합니다 (배치 서비스로 위임)
     */
    fun sendMessageBatch(tenantId: String, queueName: String, request: SendMessageBatchRequest): Mono<SendMessageBatchResponse> {
        return messageBatchService.sendMessageBatch(tenantId, queueName, request)
    }

    /**
     * 큐에서 메시지를 수신합니다
     */
    fun receiveMessages(tenantId: String, queueName: String, request: ReceiveMessageRequest): Flux<MessageResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMapMany { queue ->
                // 수신 요청 검증 (위임)
                messageValidationService.validateReceiveRequest(request, queue)

                // 통합된 메시지 서비스 사용
                val messageFlux = unifiedMessageService.receiveMessages(
                    queue = queue,
                    maxMessages = request.maxNumberOfMessages,
                    visibilityTimeoutSeconds = request.visibilityTimeoutSeconds,
                    messageGroupId = request.messageGroupId,
                    waitTimeSeconds = request.waitTimeSeconds
                )

                messageFlux.flatMap { message ->
                    // 메트릭 및 추적 처리
                    recordMessageReceived(message, tenantId, queueName, request, queue)
                }
                    .map { message ->
                        MessageResponse(
                            messageId = message.messageId,
                            messageBody = messageProcessingService.decompressMessage(message.messageBody, message.attributes),
                            attributes = message.attributes,
                            receiptHandle = message.receiptHandle!!,
                            sentTimestamp = message.sentTimestamp
                        )
                    }
                    .doOnNext { messageResponse ->
                        // 실무 필요: 개별 메시지 수신 로그
                        logger.info("Message received - tenant: $tenantId, queue: $queueName, messageId: ${messageResponse.messageId}")
                    }
            }
            .onErrorResume(handleFluxError("receiveMessages"))
    }

    /**
     * 메시지를 삭제합니다 (삭제 서비스로 위임)
     */
    fun deleteMessage(tenantId: String, queueName: String, receiptHandle: String): Mono<Void> {
        return messageDeletionService.deleteMessage(tenantId, queueName, receiptHandle)
    }

    /**
     * 큐의 모든 메시지를 삭제합니다 (삭제 서비스로 위임)
     */
    fun purgeQueue(tenantId: String, queueName: String): Mono<Void> {
        return messageDeletionService.purgeQueue(tenantId, queueName)
    }

    /**
     * 메시지를 일괄 삭제합니다 (삭제 서비스로 위임)
     */
    fun deleteMessageBatch(tenantId: String, queueName: String, request: DeleteMessageBatchRequest): Mono<DeleteMessageBatchResponse> {
        return messageDeletionService.deleteMessageBatch(tenantId, queueName, request)
    }

    /**
     * 큐에서 메시지를 제거하지 않고 내용만 미리보기합니다
     */
    fun peekMessages(tenantId: String, queueName: String, maxMessages: Int = 10): Flux<MessagePreview> {
        val validMaxMessages = maxMessages.coerceIn(1, 10)

        return queueService.findQueue(tenantId, queueName)
            .flatMapMany { queue ->
                messageRepository.peekMessages(queue, validMaxMessages)
                    .flatMap { preview ->
                        // 메시지 미리보기 추적 이벤트 기록
                        messageTraceService.recordMessagePeeked(
                            messageId = preview.messageId,
                            tenantId = tenantId,
                            queueName = queueName,
                            metadata = mapOf("maxMessages" to validMaxMessages)
                        ).thenReturn(preview)
                    }
            }
            .onErrorResume(handleFluxError("peekMessages"))
    }

    // ========== 내부 헬퍼 메서드들 ==========

    /**
     * 타겟 큐 이름 계산 (파티셔닝 고려)
     */
    private fun calculateTargetQueueName(queueName: String, request: SendMessageRequest, queue: Queue): String {
        return if (queueProperties.enablePartitioning) {
            val partitionNumber = if (queue.attributes.fifoQueue) {
                val messageGroupId = request.messageGroupId ?: request.attributes["MessageGroupId"]
                ?: throw IllegalStateException("FIFO 큐에서 MessageGroupId가 누락되었습니다")
                messageProcessingService.calculateFifoPartition(messageGroupId, queueProperties.partitionCount)
            } else {
                messageProcessingService.calculateStandardPartition(request.messageBody, queueProperties.partitionCount)
            }
            "${queueName}_partition_$partitionNumber"
        } else {
            queueName
        }
    }

    /**
     * 메시지 전송 내부 로직
     */
    private fun sendMessageInternal(
        queue: Queue,
        message: Message,
        @Suppress("UNUSED_PARAMETER") traceId: String,
        tenantId: String,
        queueName: String,
        request: SendMessageRequest,
        compressed: Boolean
    ): Mono<SendMessageResponse> {
        return unifiedMessageService.sendMessageToHot(queue, message)
            .flatMap { sentMessage ->
                // 성능 메트릭 기록
                recordMessageSent(sentMessage, tenantId, queueName, request, compressed)
                    .then(
                        // 스트리밍 이벤트 발송
                        streamingService.publishMessageSent(
                            tenantId = tenantId,
                            queueName = queueName,
                            messageId = sentMessage.messageId,
                            messageBody = request.messageBody, // 원본 메시지 본문 사용
                            attributes = sentMessage.attributes
                        )
                    )
                    .thenReturn(SendMessageResponse(messageId = sentMessage.messageId))
            }
    }

    /**
     * 메시지 전송 메트릭 기록
     */
    private fun recordMessageSent(
        message: Message,
        tenantId: String,
        queueName: String,
        request: SendMessageRequest,
        compressed: Boolean
    ): Mono<Void> {
        return Mono.fromRunnable<Void> {
            performanceMonitoringService.recordMessageSize(tenantId, queueName, message.messageBody.length)
            metricsService.recordMessageSent(tenantId, queueName, request.messageBody.length)

            if (compressed) {
                val compressionRatio = performanceMonitoringService.calculateCompressionRatio(
                    request.messageBody.length, message.messageBody.length
                )
                performanceMonitoringService.recordCompressionRatio(tenantId, queueName, compressionRatio)
            }
        }.then(
            messageTraceService.recordMessageSent(
                messageId = message.messageId,
                tenantId = tenantId,
                queueName = queueName,
                traceId = messageTraceService.generateTraceId(),
                metadata = createMessageSentMetadata(request, message, compressed)
            )
        )
    }

    /**
     * 메시지 수신 메트릭 기록
     */
    private fun recordMessageReceived(
        message: Message,
        tenantId: String,
        queueName: String,
        request: ReceiveMessageRequest,
        queue: Queue
    ): Mono<Message> {
        return Mono.fromRunnable<Void> {
            val processingTime = Random.nextLong(100, 1000)
            metricsService.recordMessageReceived(tenantId, queueName, processingTime)
        }.then(
            messageTraceService.recordMessageReceived(
                messageId = message.messageId,
                tenantId = tenantId,
                queueName = queueName,
                receiptHandle = message.receiptHandle!!,
                metadata = mapOf(
                    "receiveCount" to message.receiveCount,
                    "visibilityTimeout" to (request.visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds),
                    "queueType" to if (queue.attributes.fifoQueue) "FIFO" else "Standard"
                )
            )
        ).then(
            // 스트리밍 이벤트 발송
            streamingService.publishMessageReceived(
                tenantId = tenantId,
                queueName = queueName,
                messageId = message.messageId,
                receiptHandle = message.receiptHandle!!
            )
        ).thenReturn(message)
    }

    /**
     * 메시지 전송 메타데이터 생성
     */
    private fun createMessageSentMetadata(request: SendMessageRequest, message: Message, compressed: Boolean): Map<String, Any> {
        return mapOf(
            "messageSize" to request.messageBody.length,
            "compressedSize" to message.messageBody.length,
            "compressed" to compressed,
            "delaySeconds" to request.delaySeconds,
            "attributeCount" to request.attributes.size,
            "partition" to message.queueName
        )
    }
}
