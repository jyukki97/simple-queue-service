package com.example.sqs.queue.service

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.QueueStatsResponse
import com.example.sqs.queue.repository.QueueRepository
import org.springframework.stereotype.Service
import java.time.Instant

@Service
class QueueService(
    private val queueRepository: QueueRepository
) {
    
    fun createQueue(tenantId: String, queueName: String, attributes: QueueAttributes): Queue {
        if (queueRepository.existsByTenantIdAndQueueName(tenantId, queueName)) {
            throw IllegalArgumentException("Queue already exists: $queueName")
        }
        
        val queue = Queue(
            tenantId = tenantId,
            queueName = queueName,
            attributes = attributes,
            createdAt = Instant.now(),
            updatedAt = Instant.now()
        )
        
        return queueRepository.save(queue)
    }
    
    fun deleteQueue(tenantId: String, queueName: String) {
        val queue = findQueue(tenantId, queueName)
        queueRepository.delete(queue)
        // TODO: 큐의 모든 메시지도 삭제
    }
    
    fun findQueue(tenantId: String, queueName: String): Queue {
        return queueRepository.findByTenantIdAndQueueName(tenantId, queueName)
            ?: throw IllegalArgumentException("Queue not found: $queueName")
    }
    
    fun listQueues(tenantId: String, limit: Int, offset: Int, namePattern: String?): List<Queue> {
        return queueRepository.findByTenantId(tenantId, limit, offset, namePattern)
    }
    
    fun getQueueStats(tenantId: String, queueName: String): QueueStatsResponse {
        val queue = findQueue(tenantId, queueName)
        return queueRepository.getQueueStats(queue)
    }
    
    fun updateQueueAttributes(tenantId: String, queueName: String, attributes: QueueAttributes): Queue {
        val queue = findQueue(tenantId, queueName)
        val updatedQueue = queue.copy(
            attributes = attributes,
            updatedAt = Instant.now()
        )
        return queueRepository.save(updatedQueue)
    }
    
    fun purgeQueue(tenantId: String, queueName: String) {
        val queue = findQueue(tenantId, queueName)
        queueRepository.purgeAllMessages(queue)
    }
}
