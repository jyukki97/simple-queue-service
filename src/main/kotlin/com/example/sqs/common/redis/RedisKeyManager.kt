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
        private const val AVAILABLE_QUEUE_PREFIX = "queue:"  // 수신 대기 중인 메시지 ID 대기열 (기존 hot:queue:)
        private const val PROCESSING_QUEUE_PREFIX = "inflight:"  // 처리 중인 메시지 (가시성 타임아웃)
        private const val DELAYED_QUEUE_PREFIX = "delayed:"      // 지연 배송 메시지
        private const val DLQ_PREFIX = "dlq:"                   // 데드 레터 큐

        // Index Prefixes
        private const val QUEUE_INDEX_PREFIX = "index:queue:"
        private const val MESSAGE_INDEX_PREFIX = "index:message:"

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
     * 메인 큐 키 (수신 대기 중인 메시지 ID 저장): queue:{tenantId}:{queueName}
     */
    fun getAvailableQueueKey(tenantId: String, queueName: String): String =
        "${AVAILABLE_QUEUE_PREFIX}${tenantId}:${queueName}"

    /**
     * 메인 큐 파티션 키: queue:{tenantId}:{queueName}_partition_{partition}
     */
    fun getAvailableQueueKey(tenantId: String, queueName: String, partition: Int): String =
        "${AVAILABLE_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"

    /**
     * 메인 큐 파티션 키 (Queue 객체 사용)
     */
    fun getAvailableQueueKey(queue: Queue, partition: Int): String =
        getAvailableQueueKey(queue.tenantId, queue.queueName, partition)

    /**
     * 처리 중 메시지 큐 키: inflight:{tenantId}:{queueName}
     */
    fun getProcessingQueueKey(tenantId: String, queueName: String): String =
        "${PROCESSING_QUEUE_PREFIX}${tenantId}:${queueName}"

    /**
     * 처리 중 메시지 파티션 큐 키: inflight:{tenantId}:{queueName}_partition_{partition}
     */
    fun getProcessingQueueKey(tenantId: String, queueName: String, partition: Int): String =
        "${PROCESSING_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"

    /**
     * 처리 중 메시지 파티션 큐 키 (Queue 객체 사용)
     */
    fun getProcessingQueueKey(queue: Queue, partition: Int): String =
        getProcessingQueueKey(queue.tenantId, queue.queueName, partition)

    /**
     * 지연 메시지 큐 키: delayed:{tenantId}:{queueName}
     */
    fun getDelayedQueueKey(tenantId: String, queueName: String): String =
        "${DELAYED_QUEUE_PREFIX}${tenantId}:${queueName}"

    /**
     * 지연 메시지 파티션 큐 키: delayed:{tenantId}:{queueName}_partition_{partition}
     */
    fun getDelayedQueueKey(tenantId: String, queueName: String, partition: Int): String =
        "${DELAYED_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}${partition}"

    /**
     * 지연 메시지 파티션 큐 키 (Queue 객체 사용)
     */
    fun getDelayedQueueKey(queue: Queue, partition: Int): String =
        getDelayedQueueKey(queue.tenantId, queue.queueName, partition)

    /**
     * DLQ (Dead Letter Queue) 키: dlq:{tenantId}:{queueName}
     */
    fun getDLQKey(tenantId: String, queueName: String): String =
        "${DLQ_PREFIX}${tenantId}:${queueName}"

    // ===== Index Keys =====

    /**
     * 큐 인덱스 키: index:queue:{tenantId}:{queueName}
     */
    fun getQueueIndexKey(tenantId: String, queueName: String): String =
        "${QUEUE_INDEX_PREFIX}${tenantId}:${queueName}"

    /**
     * 메시지 인덱스 키: index:message:{tenantId}:{queueName}
     */
    fun getMessageIndexKey(tenantId: String, queueName: String): String =
        "${MESSAGE_INDEX_PREFIX}${tenantId}:${queueName}"

    // ===== Tier Index Keys =====

    /**
     * 계층별 메시지 인덱스 키: tier:index:{tier}:{tenantId}:{queueName}
     */
    fun getTierIndexKey(tier: String, tenantId: String, queueName: String): String =
        "tier:index:${tier}:${tenantId}:${queueName}"

    /**
     * 글로벌 메시지 위치 인덱스 키 (크로스-티어 쿼리용): global:index:{tenantId}:{queueName}
     */
    fun getGlobalMessageIndexKey(tenantId: String, queueName: String): String =
        "global:index:${tenantId}:${queueName}"

    /**
     * 티어 성능 인덱스 키: tier:perf:{tier}
     */
    fun getTierPerformanceIndexKey(tier: String): String =
        "tier:perf:${tier}"

    /**
     * 메시지 접근 패턴 추적 키: access:pattern:{tenantId}:{queueName}
     */
    fun getAccessPatternKey(tenantId: String, queueName: String): String =
        "access:pattern:${tenantId}:${queueName}"

    /**
     * HOT 계층 만료 메시지 인덱스 키: hot:expired:index
     */
    fun getHotExpiredIndexKey(): String = "hot:expired:index"

    /**
     * 메시지 그룹 타임라인 키 (FIFO 순서 보장용): group:{messageGroupId}:timeline
     */
    fun getMessageGroupTimelineKey(messageGroupId: String): String =
        "group:${messageGroupId}:timeline"

    /**
     * 메시지 그룹 처리 상태 키: group:{messageGroupId}:processing
     */
    fun getMessageGroupProcessingKey(messageGroupId: String): String =
        "group:${messageGroupId}:processing"

    /**
     * 메시지 중복 제거 키: dedup:{deduplicationId}
     */
    fun getDeduplicationKey(deduplicationId: String): String =
        "dedup:${deduplicationId}"

    /**
     * 계층별 메시지 인덱스 키 (Queue 객체 사용)
     */
    fun getTierIndexKey(tier: String, queue: Queue): String =
        getTierIndexKey(tier, queue.tenantId, queue.queueName)

    // ===== Metadata Keys =====

    /**
     * 큐 메타데이터 키: queue:metadata:{tenantId}:{queueName}
     */
    fun getQueueMetadataKey(tenantId: String, queueName: String): String =
        "${QUEUE_METADATA_PREFIX}${tenantId}:${queueName}"

    // ===== Pattern Matching Keys =====

    /**
     * 특정 테넌트의 모든 메인 큐 패턴: queue:{tenantId}:*
     */
    fun getAvailableQueuePattern(tenantId: String): String =
        "${AVAILABLE_QUEUE_PREFIX}${tenantId}:*"

    /**
     * 특정 테넌트/큐의 모든 파티션 패턴: queue:{tenantId}:{queueName}_partition_*
     */
    fun getAvailableQueuePartitionPattern(tenantId: String, queueName: String): String =
        "${AVAILABLE_QUEUE_PREFIX}${tenantId}:${queueName}${PARTITION_SUFFIX}*"

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
