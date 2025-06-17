package com.example.sqs.queue.repository

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.domain.QueueStatsResponse
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface QueueRepository {
    fun save(queue: Queue): Mono<Queue>
    fun findByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Queue>
    fun existsByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Boolean>
    fun findByTenantId(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue>
    fun findAllQueues(): Flux<Queue>
    fun delete(queue: Queue): Mono<Void>
    fun getQueueStats(queue: Queue): Mono<QueueStatsResponse>
    fun getQueueStatistics(queue: Queue): Mono<QueueStatistics>
    fun purgeAllMessages(queue: Queue): Mono<Void>
}
