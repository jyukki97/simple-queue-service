package com.example.sqs.common.redis

import com.example.sqs.queue.domain.Queue
import org.springframework.stereotype.Component

/**
 * Redis 키 패턴 관리를 위한 중앙화된 유틸리티
 * 모든 Redis 키 생성 로직을 통일하여 일관성 보장 및 유지보수 용이성 제공
 */
@Component
class RedisKeyManager {

    companion object {
        // Base Prefixes
        private const val MESSAGE_PREFIX = "message:"
        private const val HOT_QUEUE_PREFIX = "hot:queue:"
        private const val WARM_QUEUE_PREFIX = "warm:queue:"
        private const val COLD_QUEUE_PREFIX = "cold:queue:"
        private const val DELAYED_QUEUE_PREFIX = "delayed:queue:"
        private const val IN_FLIGHT_PREFIX = "inflight:queue:"
        private const val DLQ_PREFIX = "dlq:queue:"
        
        // Index Prefixes
        private const val HOT_INDEX_PREFIX = "hot:index:"
        private const val WARM_INDEX_PREFIX = "warm:index:"
        private const val COLD_INDEX_PREFIX = "cold:index:"
        
        // Location and Metadata
        private const val MESSAGE_LOCATION_PREFIX = "message:location:"
        private const val QUEUE_METADATA_PREFIX = "queue:metadata:"
        
        // Partition suffix
        private const val PARTITION_SUFFIX = "_partition_"
    }

    // ===== Message Keys =====
    
    /**
     * 메시지 상세 정보 키: message:{messageId}
     */
    fun getMessageKey(messageId: String): String = "${MESSAGE_PREFIX}${messageId}"
    
    /**
     * 메시지 위치 정보 키: message:location:{messageId}
     */
    fun getMessageLocationKey(messageId: String): String = "${MESSAGE_LOCATION_PREFIX}${messageId}"

    // ===== Queue Keys =====
    
    /**
     * HOT 계층 큐 키: hot:queue:{tenantId}:{queueName}
     */
    fun getHotQueueKey(tenantId: String, queueName: String): String = 
        "${HOT_QUEUE_PREFIX}${tenantId}:${queueName}"
    
    /**
     * HOT 계층 파티션 큐 키: hot:queue:{tenantId}:{queueName}_partition_{partition}
     */
    fun getHotQueueKey(tenantId: String, queueName: String, partition: Int): String = 
        "${HOT_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"
    
    /**
     * HOT 계층 파티션 큐 키 (Queue 객체 사용)
     */
    fun getHotQueueKey(queue: Queue, partition: Int): String = 
        getHotQueueKey(queue.tenantId, queue.queueName, partition)
    
    /**
     * WARM 계층 큐 키: warm:queue:{tenantId}:{queueName}
     */
    fun getWarmQueueKey(tenantId: String, queueName: String): String = 
        "${WARM_QUEUE_PREFIX}${tenantId}:${queueName}"
    
    /**
     * WARM 계층 파티션 큐 키: warm:queue:{tenantId}:{queueName}_partition_{partition}
     */
    fun getWarmQueueKey(tenantId: String, queueName: String, partition: Int): String = 
        "${WARM_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"
    
    /**
     * COLD 계층 큐 키: cold:queue:{tenantId}:{queueName}
     */
    fun getColdQueueKey(tenantId: String, queueName: String): String = 
        "${COLD_QUEUE_PREFIX}${tenantId}:${queueName}"
    
    /**
     * 지연 메시지 큐 키: delayed:queue:{tenantId}:{queueName}
     */
    fun getDelayedQueueKey(tenantId: String, queueName: String): String = 
        "${DELAYED_QUEUE_PREFIX}${tenantId}:${queueName}"
    
    /**
     * 지연 메시지 파티션 큐 키: delayed:queue:{tenantId}:{queueName}_partition_{partition}
     */
    fun getDelayedQueueKey(tenantId: String, queueName: String, partition: Int): String = 
        "${DELAYED_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"
    
    /**
     * 처리 중 메시지 큐 키: inflight:queue:{tenantId}:{queueName}
     */
    fun getInFlightQueueKey(tenantId: String, queueName: String): String = 
        "${IN_FLIGHT_PREFIX}${tenantId}:${queueName}"
    
    /**
     * 처리 중 메시지 파티션 큐 키: inflight:queue:{tenantId}:{queueName}_partition_{partition}
     */
    fun getInFlightQueueKey(tenantId: String, queueName: String, partition: Int): String = 
        "${IN_FLIGHT_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"
    
    /**
     * DLQ (Dead Letter Queue) 키: dlq:queue:{tenantId}:{queueName}
     */
    fun getDLQKey(tenantId: String, queueName: String): String = 
        "${DLQ_PREFIX}${tenantId}:${queueName}"

    // ===== Index Keys =====
    
    /**
     * HOT 계층 인덱스 키: hot:index:{tenantId}:{queueName}
     */
    fun getHotIndexKey(tenantId: String, queueName: String): String = 
        "${HOT_INDEX_PREFIX}${tenantId}:${queueName}"
    
    /**
     * WARM 계층 인덱스 키: warm:index:{tenantId}:{queueName}
     */
    fun getWarmIndexKey(tenantId: String, queueName: String): String = 
        "${WARM_INDEX_PREFIX}${tenantId}:${queueName}"
    
    /**
     * COLD 계층 인덱스 키: cold:index:{tenantId}:{queueName}
     */
    fun getColdIndexKey(tenantId: String, queueName: String): String = 
        "${COLD_INDEX_PREFIX}${tenantId}:${queueName}"

    // ===== Metadata Keys =====
    
    /**
     * 큐 메타데이터 키: queue:metadata:{tenantId}:{queueName}
     */
    fun getQueueMetadataKey(tenantId: String, queueName: String): String = 
        "${QUEUE_METADATA_PREFIX}${tenantId}:${queueName}"

    // ===== Pattern Matching Keys =====
    
    /**
     * 특정 테넌트의 모든 HOT 큐 패턴: hot:queue:{tenantId}:*
     */
    fun getHotQueuePattern(tenantId: String): String = 
        "${HOT_QUEUE_PREFIX}${tenantId}:*"
    
    /**
     * 특정 테넌트/큐의 모든 파티션 패턴: hot:queue:{tenantId}:{queueName}_partition_*
     */
    fun getHotQueuePartitionPattern(tenantId: String, queueName: String): String = 
        "${HOT_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}*"
    
    /**
     * 모든 메시지 위치 패턴: message:location:*
     */
    fun getMessageLocationPattern(): String = "${MESSAGE_LOCATION_PREFIX}*"
    
    /**
     * 특정 큐의 모든 메시지 위치 패턴: message:location:*{queueIdentifier}*
     */
    fun getMessageLocationPattern(tenantId: String, queueName: String): String = 
        "${MESSAGE_LOCATION_PREFIX}*${tenantId}:${queueName}*"
    
    /**
     * 모든 메시지 키 패턴: message:*
     */
    fun getMessagePattern(): String = "${MESSAGE_PREFIX}*"

    // ===== 파티션 추출 유틸리티 =====
    
    /**
     * 키에서 파티션 번호 추출
     */
    fun extractPartitionNumber(key: String): Int? {
        val partitionIndex = key.lastIndexOf(PARTITION_SUFFIX)
        return if (partitionIndex != -1) {
            val partitionStr = key.substring(partitionIndex + PARTITION_SUFFIX.length)
            partitionStr.toIntOrNull()
        } else null
    }
    
    /**
     * 파티션 키인지 확인
     */
    fun isPartitionKey(key: String): Boolean = key.contains(PARTITION_SUFFIX)
    
    /**
     * 기본 큐 이름에서 파티션 제거
     */
    fun getBaseQueueName(queueName: String): String {
        val partitionIndex = queueName.lastIndexOf(PARTITION_SUFFIX)
        return if (partitionIndex != -1) {
            queueName.substring(0, partitionIndex)
        } else queueName
    }
    
    // ===== Metrics Keys =====
    
    /**
     * 메트릭스 키 (HOT 계층): hot:metrics
     */
    fun getHotMetricsKey(): String = "hot:metrics"
    
    /**
     * 메트릭스 키 (WARM 계층): warm:metrics  
     */
    fun getWarmMetricsKey(): String = "warm:metrics"
    
    /**
     * 메트릭스 키 (COLD 계층): cold:metrics
     */
    fun getColdMetricsKey(): String = "cold:metrics"

    /**
     * 메시지 키 접두사: message:
     */
    fun getMessagePrefix(): String = MESSAGE_PREFIX
}
