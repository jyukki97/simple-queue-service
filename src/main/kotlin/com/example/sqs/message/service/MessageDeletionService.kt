package com.example.sqs.message.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.message.domain.BatchMessageFailure
import com.example.sqs.message.domain.DeleteMessageBatchRequest
import com.example.sqs.message.domain.DeleteMessageBatchResponse
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.queue.service.QueueService
import com.example.sqs.streaming.service.StreamingService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * 메시지 삭제 관련 기능을 담당하는 서비스
 */
@Service
class MessageDeletionService(
    private val messageRepository: MessageRepository,
    private val queueService: QueueService,
    private val streamingService: StreamingService
) : BaseReactiveService() {

    override fun getServiceName(): String = "MessageDeletionService"

    /**
     * 메시지를 삭제합니다
     */
    fun deleteMessage(tenantId: String, queueName: String, receiptHandle: String): Mono<Void> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue -> 
                messageRepository.deleteMessage(queue, receiptHandle)
                    .then(
                        // 스트리밍 이벤트 발송 (messageId는 receiptHandle을 임시로 사용)
                        streamingService.publishMessageDeleted(
                            tenantId = tenantId,
                            queueName = queueName,
                            messageId = receiptHandle // 임시로 receiptHandle 사용
                        )
                    )
            }
            .doOnSuccess {
                logger.info("[${getServiceName()}] 메시지 삭제 완료 - tenant: $tenantId, queue: $queueName")
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

                // 각 receiptHandle에 대해 삭제 처리
                Flux.fromIterable(request.receiptHandles)
                    .flatMap { receiptHandle ->
                        messageRepository.deleteMessage(queue, receiptHandle)
                            .then(Mono.defer {
                                Mono.fromRunnable<Void> {
                                    successful.add(receiptHandle)
                                }
                            })
                            .onErrorResume { e: Throwable ->
                                logger.error("배치 메시지 삭제 실패: $receiptHandle", e)
                                Mono.fromRunnable<Void> {
                                    failed.add(
                                        BatchMessageFailure(
                                            id = receiptHandle,
                                            code = "DeleteError",
                                            message = e.message ?: "Unknown error"
                                        )
                                    )
                                }
                            }
                    }
                    .then(
                        Mono.fromCallable {
                            logger.info("[${getServiceName()}] 배치 삭제 완료 - tenant: $tenantId, queue: $queueName, success: ${successful.size}, failed: ${failed.size}")
                            DeleteMessageBatchResponse(successful = successful, failed = failed)
                        }
                    )
            }
    }

    /**
     * 큐의 모든 메시지를 삭제합니다
     */
    fun purgeQueue(tenantId: String, queueName: String): Mono<Void> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue -> messageRepository.purgeAllMessages(queue) }
            .then()
    }
}
