package com.example.sqs.message.service

import com.example.sqs.message.domain.Message
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.domain.SendMessageResponse
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.MessageResponse
import com.example.sqs.message.domain.SendMessageBatchRequest
import com.example.sqs.message.domain.SendMessageBatchResponse
import com.example.sqs.message.domain.BatchMessageResponse
import com.example.sqs.message.domain.BatchMessageFailure
import com.example.sqs.message.domain.DeleteMessageBatchRequest
import com.example.sqs.message.domain.DeleteMessageBatchResponse
import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.queue.service.QueueService
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.*

@Service
class MessageService(
    private val messageRepository: MessageRepository,
    private val queueService: QueueService
) {

    /**
     * 큐에 메시지를 전송합니다
     */
    fun sendMessage(tenantId: String, queueName: String, request: SendMessageRequest): Mono<SendMessageResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                val visibleAt = if (request.delaySeconds > 0) {
                    Instant.now().plusSeconds(request.delaySeconds.toLong())
                } else {
                    Instant.now()
                }
                
                val message = Message(
                    tenantId = tenantId,
                    queueName = queueName,
                    messageBody = request.messageBody,
                    attributes = request.attributes,
                    sentTimestamp = Instant.now(),
                    visibleAt = visibleAt
                )
                
                messageRepository.sendMessage(queue, message)
                    .map { SendMessageResponse(messageId = it.messageId) }
            }
    }

    /**
     * 큐에 메시지를 일괄 전송합니다
     */
    fun sendMessageBatch(tenantId: String, queueName: String, request: SendMessageBatchRequest): Mono<SendMessageBatchResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                val successful = mutableListOf<BatchMessageResponse>()
                val failed = mutableListOf<BatchMessageFailure>()
                
                Flux.fromIterable(request.messages)
                    .flatMap { batchRequest ->
                        try {
                            val visibleAt = if (batchRequest.delaySeconds > 0) {
                                Instant.now().plusSeconds(batchRequest.delaySeconds.toLong())
                            } else {
                                Instant.now()
                            }
                            
                            val message = Message(
                                tenantId = tenantId,
                                queueName = queueName,
                                messageBody = batchRequest.messageBody,
                                attributes = batchRequest.attributes,
                                sentTimestamp = Instant.now(),
                                visibleAt = visibleAt
                            )
                            
                            messageRepository.sendMessage(queue, message)
                                .map { sentMessage ->
                                    successful.add(
                                        BatchMessageResponse(
                                            id = batchRequest.id,
                                            messageId = sentMessage.messageId
                                        )
                                    )
                                }
                                .onErrorResume { e ->
                                    failed.add(
                                        BatchMessageFailure(
                                            id = batchRequest.id,
                                            code = "InvalidParameterValue",
                                            message = e.message ?: "메시지 전송 실패"
                                        )
                                    )
                                    Mono.empty()
                                }
                        } catch (e: Exception) {
                            failed.add(
                                BatchMessageFailure(
                                    id = batchRequest.id,
                                    code = "InvalidParameterValue",
                                    message = e.message ?: "잘못된 메시지 형식"
                                )
                            )
                            Mono.empty()
                        }
                    }
                    .then(
                        Mono.just(
                            SendMessageBatchResponse(
                                successful = successful,
                                failed = failed
                            )
                        )
                    )
            }
    }

    /**
     * 큐에서 메시지를 수신합니다
     */
    fun receiveMessages(tenantId: String, queueName: String, request: ReceiveMessageRequest): Flux<MessageResponse> {
        return queueService.findQueue(tenantId, queueName)
            .flatMapMany { queue ->
                messageRepository.receiveMessages(queue, request.maxNumberOfMessages, request.visibilityTimeoutSeconds)
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
                messageRepository.deleteMessage(queue, receiptHandle)
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
}
