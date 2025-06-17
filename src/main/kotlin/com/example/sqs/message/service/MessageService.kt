package com.example.sqs.message.service

import com.example.sqs.message.domain.Message
import com.example.sqs.message.domain.MessagePreview
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.domain.SendMessageResponse
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.MessageResponse
import com.example.sqs.message.domain.SendMessageBatchRequest
import com.example.sqs.message.domain.SendMessageBatchResponse
import com.example.sqs.message.domain.BatchMessageResponse
import com.example.sqs.message.domain.BatchMessageFailure
import com.example.sqs.message.domain.BatchMessageRequest
import com.example.sqs.message.domain.DeleteMessageBatchRequest
import com.example.sqs.message.domain.DeleteMessageBatchResponse
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.queue.service.QueueService
import com.example.sqs.tracing.service.MessageTraceService
import com.example.sqs.metrics.service.MetricsService
import com.example.sqs.performance.compression.MessageCompressionService
import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.performance.batch.RedisBatchService
import com.example.sqs.performance.monitoring.PerformanceMonitoringService
import com.example.sqs.config.QueueProperties
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*
import java.util.concurrent.atomic.AtomicLong

@Service
class MessageService(
    private val messageRepository: MessageRepository,
    private val queueService: QueueService,
    private val messageTraceService: MessageTraceService,
    private val metricsService: MetricsService,
    private val compressionService: MessageCompressionService,
    private val partitioningService: QueuePartitioningService,
    private val batchService: RedisBatchService,
    private val performanceMonitoringService: PerformanceMonitoringService,
    private val queueProperties: QueueProperties
) {
    
    private val partitionCounter = AtomicLong(0)

    /**
     * 큐에 메시지를 전송합니다 (성능 최적화 적용)
     */
    fun sendMessage(tenantId: String, queueName: String, request: SendMessageRequest): Mono<SendMessageResponse> {
        val traceId = messageTraceService.generateTraceId()
        
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                // 메시지 압축 처리
                val (messageBody, compressed) = processMessageCompression(request.messageBody)
                
                // 파티셔닝 적용
                val targetQueueName = if (queueProperties.enablePartitioning) {
                    val partitionNumber = getPartitionForMessage(request.messageBody)
                    partitioningService.getPartitionedQueueName(queueName, partitionNumber)
                } else {
                    queueName
                }
                
                val visibleAt = if (request.delaySeconds > 0) {
                    Instant.now().plusSeconds(request.delaySeconds.toLong())
                } else {
                    Instant.now()
                }
                
                val enhancedAttributes = request.attributes.toMutableMap().apply {
                    if (compressed) {
                        put("_compressed", "true")
                        put("_original_size", request.messageBody.length.toString())
                    }
                    put("_partition", targetQueueName)
                }
                
                val message = Message(
                    tenantId = tenantId,
                    queueName = targetQueueName,
                    messageBody = messageBody,
                    attributes = enhancedAttributes,
                    sentTimestamp = Instant.now(),
                    visibleAt = visibleAt
                )
                
                messageRepository.sendMessage(queue, message)
                    .flatMap { sentMessage ->
                        // 성능 메트릭 기록
                        performanceMonitoringService.recordMessageSize(
                            tenantId, queueName, messageBody.length
                        )
                        
                        if (compressed) {
                            val compressionRatio = compressionService.calculateCompressionRatio(
                                request.messageBody.length, messageBody.length
                            )
                            performanceMonitoringService.recordCompressionRatio(
                                tenantId, queueName, compressionRatio
                            )
                        }
                        
                        // 기존 메트릭 기록
                        metricsService.recordMessageSent(tenantId, queueName, request.messageBody.length)
                        
                        // 메시지 전송 추적 이벤트 기록
                        messageTraceService.recordMessageSent(
                            messageId = sentMessage.messageId,
                            tenantId = tenantId,
                            queueName = queueName,
                            traceId = traceId,
                            metadata = mapOf(
                                "messageSize" to request.messageBody.length,
                                "compressedSize" to messageBody.length,
                                "compressed" to compressed,
                                "delaySeconds" to request.delaySeconds,
                                "attributeCount" to request.attributes.size,
                                "partition" to targetQueueName
                            )
                        ).thenReturn(sentMessage)
                    }
                    .map { SendMessageResponse(messageId = it.messageId) }
                    .doOnSuccess { 
                        performanceMonitoringService.incrementCounter("sendMessage", tenantId, queueName)
                    }
                    .doOnError { error ->
                        performanceMonitoringService.incrementErrorCounter(
                            "sendMessage", tenantId, error.javaClass.simpleName, queueName
                        )
                    }
            }
    }

    /**
     * 큐에 메시지를 일괄 전송합니다 (성능 최적화 적용)
     */
    fun sendMessageBatch(tenantId: String, queueName: String, request: SendMessageBatchRequest): Mono<SendMessageBatchResponse> {
        val startTime = System.currentTimeMillis()
        
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                val successful = mutableListOf<BatchMessageResponse>()
                val failed = mutableListOf<BatchMessageFailure>()
                
                // 배치 처리를 위한 메시지 그룹핑 (파티션별)
                val messagesByPartition = if (queueProperties.enablePartitioning) {
                    request.messages.groupBy { batchRequest ->
                        getPartitionForMessage(batchRequest.messageBody)
                    }
                } else {
                    mapOf(0 to request.messages)
                }
                
                // 파티션별로 배치 처리
                Flux.fromIterable(messagesByPartition.entries)
                    .flatMap { (partitionNumber, messages) ->
                        val targetQueueName = if (queueProperties.enablePartitioning) {
                            partitioningService.getPartitionedQueueName(queueName, partitionNumber)
                        } else {
                            queueName
                        }
                        
                        processBatchMessages(tenantId, targetQueueName, queue, messages, successful, failed)
                    }
                    .then(
                        Mono.fromCallable {
                            val processingTime = System.currentTimeMillis() - startTime
                            
                            // 배치 처리 효율성 모니터링
                            performanceMonitoringService.recordBatchEfficiency(
                                tenantId, "sendMessage", request.messages.size, processingTime
                            )
                            
                            SendMessageBatchResponse(
                                successful = successful,
                                failed = failed
                            )
                        }
                    )
                    .doOnSuccess {
                        performanceMonitoringService.incrementCounter("sendMessageBatch", tenantId, queueName)
                    }
                    .doOnError { error ->
                        performanceMonitoringService.incrementErrorCounter(
                            "sendMessageBatch", tenantId, error.javaClass.simpleName, queueName
                        )
                    }
            }
    }

    /**
     * 큐에서 메시지를 수신합니다
     */
    fun receiveMessages(tenantId: String, queueName: String, request: ReceiveMessageRequest): Flux<MessageResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMapMany { queue ->
                messageRepository.receiveMessages(queue, request.maxNumberOfMessages, request.visibilityTimeoutSeconds)
                    .flatMap { message ->
                        // 메트릭 기록 (처리 시간은 임의로 계산)
                        val processingTime = kotlin.random.Random.nextLong(100, 1000)
                        metricsService.recordMessageReceived(tenantId, queueName, processingTime)
                        
                        // 메시지 수신 추적 이벤트 기록
                        messageTraceService.recordMessageReceived(
                            messageId = message.messageId,
                            tenantId = tenantId,
                            queueName = queueName,
                            receiptHandle = message.receiptHandle!!,
                            metadata = mapOf(
                                "receiveCount" to message.receiveCount,
                                "visibilityTimeout" to (request.visibilityTimeoutSeconds ?: queue.attributes.visibilityTimeoutSeconds)
                            )
                        ).thenReturn(message)
                    }
                    .map { message ->
                        MessageResponse(
                            messageId = message.messageId,
                            messageBody = message.messageBody,
                            attributes = message.attributes,
                            receiptHandle = message.receiptHandle!!,
                            sentTimestamp = message.sentTimestamp
                        )
                    }
            }
    }

    /**
     * 메시지를 삭제합니다
     */
    fun deleteMessage(tenantId: String, queueName: String, receiptHandle: String): Mono<Void> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                // 먼저 메시지 정보를 조회해서 messageId를 얻은 후 삭제
                // 여기서는 간단히 receiptHandle을 사용 (실제로는 Redis에서 조회 필요)
                val messageId = receiptHandle.split(":").getOrNull(0) ?: receiptHandle
                
                messageRepository.deleteMessage(queue, receiptHandle)
                    .then(
                        Mono.fromRunnable<Void> {
                            // 메트릭 기록
                            metricsService.recordMessageDeleted(tenantId, queueName)
                        }
                    )
                    .then(
                        // 메시지 삭제 추적 이벤트 기록
                        messageTraceService.recordMessageDeleted(
                            messageId = messageId,
                            tenantId = tenantId,
                            queueName = queueName,
                            receiptHandle = receiptHandle
                        )
                    )
            }
    }

    /**
     * 큐의 모든 메시지를 삭제합니다
     */
    fun purgeQueue(tenantId: String, queueName: String): Mono<Void> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                messageRepository.purgeAllMessages(queue)
            }
    }

    /**
     * 메시지를 일괄 삭제합니다
     */
    fun deleteMessageBatch(tenantId: String, queueName: String, request: DeleteMessageBatchRequest): Mono<DeleteMessageBatchResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                val successful = mutableListOf<String>()
                val failed = mutableListOf<BatchMessageFailure>()
                
                Flux.fromIterable(request.receiptHandles)
                    .flatMap { receiptHandle ->
                        messageRepository.deleteMessage(queue, receiptHandle)
                            .doOnSuccess { successful.add(receiptHandle) }
                            .onErrorResume { e ->
                                failed.add(
                                    BatchMessageFailure(
                                        id = receiptHandle,
                                        code = "ReceiptHandleIsInvalid",
                                        message = e.message ?: "잘못된 receipt handle입니다"
                                    )
                                )
                                Mono.empty<Void>()
                            }
                    }
                    .then()
                    .map {
                        DeleteMessageBatchResponse(
                            successful = successful,
                            failed = failed
                        )
                    }
            }
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
                            metadata = mapOf(
                                "maxMessages" to validMaxMessages
                            )
                        ).thenReturn(preview)
                    }
            }
    }
    
    // ========== 성능 최적화 헬퍼 메서드들 ==========
    
    /**
     * 메시지 압축 처리
     */
    private fun processMessageCompression(messageBody: String): Pair<String, Boolean> {
        return if (queueProperties.enableCompression && 
                   compressionService.shouldCompress(messageBody, queueProperties.compressionThreshold)) {
            val compressed = compressionService.compress(messageBody)
            compressed.toString(Charsets.ISO_8859_1) to true
        } else {
            messageBody to false
        }
    }
    
    /**
     * 메시지 압축 해제
     */
    private fun decompressMessage(messageBody: String, attributes: Map<String, String>): String {
        return if (attributes["_compressed"] == "true") {
            val compressedData = messageBody.toByteArray(Charsets.ISO_8859_1)
            compressionService.decompress(compressedData)
        } else {
            messageBody
        }
    }
    
    /**
     * 메시지 파티션 결정
     */
    private fun getPartitionForMessage(messageBody: String): Int {
        return if (queueProperties.enablePartitioning) {
            // 메시지 내용의 해시값을 기반으로 파티션 결정
            partitioningService.getPartitionNumber(messageBody, queueProperties.partitionCount)
        } else {
            0
        }
    }
    
    /**
     * 배치 메시지 처리
     */
    private fun processBatchMessages(
        tenantId: String,
        targetQueueName: String,
        queue: com.example.sqs.queue.domain.Queue,
        messages: List<BatchMessageRequest>,
        successful: MutableList<BatchMessageResponse>,
        failed: MutableList<BatchMessageFailure>
    ): Mono<Void> {
        return Flux.fromIterable(messages)
            .flatMap { batchRequest ->
                try {
                    // 메시지 압축 처리
                    val (messageBody, compressed) = processMessageCompression(batchRequest.messageBody)
                    
                    val visibleAt = if (batchRequest.delaySeconds > 0) {
                        Instant.now().plusSeconds(batchRequest.delaySeconds.toLong())
                    } else {
                        Instant.now()
                    }
                    
                    val enhancedAttributes = batchRequest.attributes.toMutableMap()
                    if (compressed) {
                        enhancedAttributes["_compressed"] = "true"
                        enhancedAttributes["_original_size"] = batchRequest.messageBody.length.toString()
                    }
                    enhancedAttributes["_partition"] = targetQueueName
                    
                    val message = Message(
                        tenantId = tenantId,
                        queueName = targetQueueName,
                        messageBody = messageBody,
                        attributes = enhancedAttributes,
                        sentTimestamp = Instant.now(),
                        visibleAt = visibleAt
                    )
                    
                    messageRepository.sendMessage(queue, message)
                        .map { sentMessage ->
                            synchronized(successful) {
                                successful.add(
                                    BatchMessageResponse(
                                        id = batchRequest.id,
                                        messageId = sentMessage.messageId
                                    )
                                )
                            }
                            
                            // 성능 메트릭 기록
                            performanceMonitoringService.recordMessageSize(
                                tenantId, targetQueueName, messageBody.length
                            )
                            
                            if (compressed) {
                                val compressionRatio = compressionService.calculateCompressionRatio(
                                    batchRequest.messageBody.length, messageBody.length
                                )
                                performanceMonitoringService.recordCompressionRatio(
                                    tenantId, targetQueueName, compressionRatio
                                )
                            }
                        }
                        .onErrorResume { e ->
                            synchronized(failed) {
                                failed.add(
                                    BatchMessageFailure(
                                        id = batchRequest.id,
                                        code = "InvalidParameterValue",
                                        message = e.message ?: "메시지 전송 실패"
                                    )
                                )
                            }
                            Mono.empty()
                        }
                } catch (e: Exception) {
                    synchronized(failed) {
                        failed.add(
                            BatchMessageFailure(
                                id = batchRequest.id,
                                code = "InvalidParameterValue",
                                message = e.message ?: "잘못된 메시지 형식"
                            )
                        )
                    }
                    Mono.empty()
                }
            }
            .then()
    }
}
