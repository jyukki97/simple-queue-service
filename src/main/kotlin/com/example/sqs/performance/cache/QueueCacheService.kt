package com.example.sqs.performance.cache

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueStatistics
import kotlinx.coroutines.reactor.awaitSingle
import kotlinx.coroutines.reactor.awaitSingleOrNull
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration

/**
 * 큐 메타데이터 및 통계 캐싱 서비스
 */
@Component
class QueueCacheService(
    private val redisTemplate: ReactiveRedisTemplate<String, Any>
) {
    
    companion object {
        private const val QUEUE_METADATA_PREFIX = "cache:queue:metadata"
        private const val QUEUE_STATS_PREFIX = "cache:queue:stats"
        private const val TENANT_QUEUES_PREFIX = "cache:tenant:queues"
        
        private val METADATA_TTL = Duration.ofMinutes(30)
        private val STATS_TTL = Duration.ofMinutes(5)
        private val TENANT_QUEUES_TTL = Duration.ofMinutes(15)
    }
    
    /**
     * 큐 메타데이터 캐시 조회
     */
    suspend fun getQueueMetadata(tenantId: String, queueName: String): Queue? {
        val key = "${QUEUE_METADATA_PREFIX}:${tenantId}:${queueName}"
        return redisTemplate.opsForValue()
            .get(key)
            .cast(Queue::class.java)
            .awaitSingleOrNull()
    }
    
    /**
     * 큐 메타데이터 캐시 저장
     */
    suspend fun cacheQueueMetadata(queue: Queue) {
        val key = "${QUEUE_METADATA_PREFIX}:${queue.tenantId}:${queue.queueName}"
        redisTemplate.opsForValue()
            .set(key, queue, METADATA_TTL)
            .awaitSingle()
    }
    
    /**
     * 큐 메타데이터 캐시 삭제
     */
    suspend fun evictQueueMetadata(tenantId: String, queueName: String) {
        val key = "${QUEUE_METADATA_PREFIX}:${tenantId}:${queueName}"
        redisTemplate.delete(key).awaitSingle()
    }
    
    /**
     * 큐 통계 캐시 조회
     */
    suspend fun getQueueStats(tenantId: String, queueName: String): QueueStatistics? {
        val key = "${QUEUE_STATS_PREFIX}:${tenantId}:${queueName}"
        return redisTemplate.opsForValue()
            .get(key)
            .cast(QueueStatistics::class.java)
            .awaitSingleOrNull()
    }
    
    /**
     * 큐 통계 캐시 저장
     */
    suspend fun cacheQueueStats(tenantId: String, queueName: String, stats: QueueStatistics) {
        val key = "${QUEUE_STATS_PREFIX}:${tenantId}:${queueName}"
        redisTemplate.opsForValue()
            .set(key, stats, STATS_TTL)
            .awaitSingle()
    }
    
    /**
     * 테넌트의 큐 목록 캐시 조회
     */
    suspend fun getTenantQueues(tenantId: String): Set<String>? {
        val key = "${TENANT_QUEUES_PREFIX}:${tenantId}"
        return redisTemplate.opsForSet()
            .members(key)
            .cast(String::class.java)
            .collectList()
            .map { it.toSet() }
            .awaitSingleOrNull()
    }
    
    /**
     * 테넌트의 큐 목록 캐시 저장
     */
    suspend fun cacheTenantQueues(tenantId: String, queueNames: Set<String>) {
        val key = "${TENANT_QUEUES_PREFIX}:${tenantId}"
        
        // 기존 캐시 삭제
        redisTemplate.delete(key).awaitSingle()
        
        // 새 데이터 저장
        if (queueNames.isNotEmpty()) {
            redisTemplate.opsForSet()
                .add(key, *queueNames.toTypedArray())
                .awaitSingle()
            
            redisTemplate.expire(key, TENANT_QUEUES_TTL).awaitSingle()
        }
    }
    
    /**
     * 테넌트 큐 목록에 큐 추가
     */
    suspend fun addQueueToTenantCache(tenantId: String, queueName: String) {
        val key = "${TENANT_QUEUES_PREFIX}:${tenantId}"
        redisTemplate.opsForSet()
            .add(key, queueName)
            .awaitSingle()
        
        redisTemplate.expire(key, TENANT_QUEUES_TTL).awaitSingle()
    }
    
    /**
     * 테넌트 큐 목록에서 큐 제거
     */
    suspend fun removeQueueFromTenantCache(tenantId: String, queueName: String) {
        val key = "${TENANT_QUEUES_PREFIX}:${tenantId}"
        redisTemplate.opsForSet()
            .remove(key, queueName)
            .awaitSingle()
    }
    
    /**
     * 모든 캐시 무효화
     */
    suspend fun evictAllCaches(tenantId: String) {
        val patterns = listOf(
            "${QUEUE_METADATA_PREFIX}:${tenantId}:*",
            "${QUEUE_STATS_PREFIX}:${tenantId}:*",
            "${TENANT_QUEUES_PREFIX}:${tenantId}"
        )
        
        patterns.forEach { pattern ->
            redisTemplate.keys(pattern)
                .collectList()
                .flatMap { keys -> 
                    if (keys.isNotEmpty()) {
                        redisTemplate.delete(Flux.fromIterable(keys))
                    } else {
                        Mono.just(0L)
                    }
                }
                .awaitSingle()
        }
    }
    
    /**
     * 캐시 워밍업 - 자주 사용되는 데이터 미리 로드
     */
    suspend fun warmupCache(tenantId: String, queueNames: List<String>) {
        // 테넌트 큐 목록 미리 캐시
        cacheTenantQueues(tenantId, queueNames.toSet())
        
        // 각 큐의 메타데이터는 실제 요청 시 캐시됨
    }
}

/**
 * 캐시 통계 정보
 */
data class CacheStatistics(
    val hitCount: Long,
    val missCount: Long,
    val evictionCount: Long,
    val hitRatio: Double
) {
    companion object {
        fun calculateHitRatio(hitCount: Long, missCount: Long): Double {
            val totalRequests = hitCount + missCount
            return if (totalRequests > 0) {
                hitCount.toDouble() / totalRequests
            } else {
                0.0
            }
        }
    }
}
