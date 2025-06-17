package com.example.sqs.queue.repository

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatsResponse

interface QueueRepository {
    fun save(queue: Queue): Queue
    fun findByTenantIdAndQueueName(tenantId: String, queueName: String): Queue?
    fun existsByTenantIdAndQueueName(tenantId: String, queueName: String): Boolean
    fun findByTenantId(tenantId: String, limit: Int, offset: Int, namePattern: String?): List<Queue>
    fun delete(queue: Queue)
    fun getQueueStats(queue: Queue): QueueStatsResponse
    fun purgeAllMessages(queue: Queue)
}
