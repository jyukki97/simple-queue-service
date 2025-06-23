package com.example.sqs.message.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.config.QueueProperties
import com.example.sqs.message.domain.*
import com.example.sqs.message.factory.MessageFactory
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.performance.monitoring.PerformanceMonitoringService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.service.QueueService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * 메시지 배치 처리 관련 기능을 담당하는 서비스
 */
@Service
class MessageBatchService(
    private val messageRepository: MessageRepository,
    private val queueService: QueueService,
    private val performanceMonitoringService: PerformanceMonitoringService,
    private val queueProperties: QueueProperties,
    private val messageValidationService: MessageValidationService,
    private val messageProcessingService: MessageProcessingService,
    private val messageFactory: MessageFactory
) : BaseReactiveService() {

    override fun getServiceName(): String = "MessageBatchService"

    /**
     * 큐에 메시지를 일괄 전송합니다
     */
    fun sendMessageBatch(tenantId: String, queueName: String, request: SendMessageBatchRequest): Mono<SendMessageBatchResponse> {
        val startTime = System.currentTimeMillis()

        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                val successful = mutableListOf<BatchMessageResponse>()
                val failed = mutableListOf<BatchMessageFailure>()

                // 배치 처리를 위한 메시지 그룹핑 (파티션별)
                val messagesByPartition = groupMessagesByPartition(request.messages, queue)

                // 파티션별로 배치 처리
                Flux.fromIterable(messagesByPartition.entries)
                    .flatMap { (partitionNumber, messages) ->
                        val targetQueueName = calculateTargetQueueName(queueName, partitionNumber)
                        processBatchMessages(tenantId, targetQueueName, queue, messages, successful, failed)
                    }
                    .then(
                        Mono.fromCallable {
                            recordBatchMetrics(
                                tenantId, queueName, request.messages.size,
                                successful.size, failed.size, startTime
                            )

                            SendMessageBatchResponse(
                                successful = successful,
                                failed = failed
                            )
                        }
                    )
            }
            .onErrorResume(handleError("sendMessageBatch"))
    }

    /**
     * 메시지를 파티션별로 그룹핑
     */
    private fun groupMessagesByPartition(messages: List<BatchMessageRequest>, queue: Queue): Map<Int, List<BatchMessageRequest>> {
        return messages.groupBy { batchRequest ->
            val partition = if (queue.attributes.fifoQueue) {
                val messageGroupId = batchRequest.messageGroupId ?: batchRequest.attributes["MessageGroupId"]
                ?: throw IllegalArgumentException("FIFO 큐에서 MessageGroupId가 누락되었습니다: ${batchRequest.id}")
                messageProcessingService.calculateFifoPartition(messageGroupId, queueProperties.partitionCount)
            } else {
                messageProcessingService.calculateStandardPartition(batchRequest.messageBody, queueProperties.partitionCount)
            }
            partition
        }
    }

    /**
     * 배치 메시지 처리
     */
    private fun processBatchMessages(
        tenantId: String,
        targetQueueName: String,
        queue: Queue,
        messages: List<BatchMessageRequest>,
        successful: MutableList<BatchMessageResponse>,
        failed: MutableList<BatchMessageFailure>
    ): Mono<Void> {
        return Flux.fromIterable(messages)
            .flatMap { batchRequest ->
                processSingleBatchMessage(tenantId, targetQueueName, queue, batchRequest, successful, failed)
            }
            .then()
    }

    /**
     * 단일 배치 메시지 처리
     */
    private fun processSingleBatchMessage(
        tenantId: String,
        targetQueueName: String,
        queue: Queue,
        batchRequest: BatchMessageRequest,
        successful: MutableList<BatchMessageResponse>,
        failed: MutableList<BatchMessageFailure>
    ): Mono<Void> {
        return Mono.fromCallable {
            // 배치 메시지 검증
            messageValidationService.validateBatchMessage(batchRequest, queue)

            // 메시지 압축 처리
            val (messageBody, compressed) = messageProcessingService.processMessageCompression(batchRequest.messageBody)

            // 메시지 생성
            val message = messageFactory.createMessage(
                tenantId = tenantId,
                targetQueueName = targetQueueName,
                messageBody = messageBody,
                attributes = batchRequest.attributes,
                delaySeconds = batchRequest.delaySeconds,
                messageGroupId = batchRequest.messageGroupId,
                messageDeduplicationId = batchRequest.messageDeduplicationId,
                compressed = compressed,
                originalMessageBodyLength = if (compressed) batchRequest.messageBody.length else null
            )
            message
        }.flatMap { message ->
            // 메시지 전송
            messageRepository.sendMessage(queue, message)
                .flatMap { sentMessage: Message ->
                    Mono.fromRunnable<Void> {
                        successful.add(
                            BatchMessageResponse(
                                messageId = sentMessage.messageId,
                                id = batchRequest.id
                            )
                        )
                    }
                }
        }
            .onErrorResume { e: Throwable ->
                logger.error("배치 메시지 처리 실패: ${batchRequest.id}", e)
                Mono.fromRunnable {
                    failed.add(
                        BatchMessageFailure(
                            id = batchRequest.id,
                            code = "ProcessingError",
                            message = e.message ?: "Unknown error"
                        )
                    )
                }
            }
            .then()
    }

    /**
     * 타겟 큐 이름 계산
     */
    private fun calculateTargetQueueName(queueName: String, partitionNumber: Int): String {
        return if (queueProperties.enablePartitioning) {
            "${queueName}_partition_$partitionNumber"
        } else {
            queueName
        }
    }

    /**
     * 배치 처리 메트릭 기록
     */
    private fun recordBatchMetrics(
        tenantId: String,
        queueName: String,
        totalMessages: Int,
        successCount: Int,
        failedCount: Int,
        startTime: Long
    ) {
        val processingTime = System.currentTimeMillis() - startTime

        performanceMonitoringService.recordBatchProcessingTime(tenantId, queueName, processingTime)
        performanceMonitoringService.recordBatchProcessingResults(tenantId, queueName, successCount, failedCount)

        logger.info("배치 처리 완료: tenant=$tenantId, queue=$queueName, total=$totalMessages, success=$successCount, failed=$failedCount, time=${processingTime}ms")
    }
}
