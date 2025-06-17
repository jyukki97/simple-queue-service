package com.example.sqs.message.repository

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface MessageRepository {
    /**
     * 메시지를 큐에 전송합니다
     */
    fun sendMessage(queue: Queue, message: Message): Mono<Message>
    
    /**
     * 큐에서 메시지를 수신합니다
     */
    fun receiveMessages(queue: Queue, maxMessages: Int, visibilityTimeoutSeconds: Int?): Flux<Message>
    
    /**
     * 메시지를 삭제합니다
     */
    fun deleteMessage(queue: Queue, receiptHandle: String): Mono<Void>
    
    /**
     * 메시지 ID로 메시지를 조회합니다
     */
    fun findByMessageId(messageId: String): Mono<Message>
    
    /**
     * 큐의 모든 메시지를 삭제합니다 (PurgeQueue용)
     */
    fun purgeAllMessages(queue: Queue): Mono<Void>
    
    /**
     * 가시성 타임아웃이 만료된 메시지들을 다시 available 상태로 만듭니다
     */
    fun restoreExpiredMessages(queue: Queue): Mono<Long>
    
    /**
     * 지연된 메시지들 중 실행 시간이 된 것들을 available 상태로 이동합니다
     */
    fun moveDelayedToAvailable(queue: Queue): Mono<Long>
}
