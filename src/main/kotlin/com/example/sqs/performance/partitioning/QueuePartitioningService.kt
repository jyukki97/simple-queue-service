package com.example.sqs.performance.partitioning

import org.springframework.stereotype.Component
import kotlin.math.abs

/**
 * 큐 파티셔닝 서비스
 * 높은 처리량을 위해 큐를 여러 파티션으로 분할
 */
@Component
class QueuePartitioningService {
    
    /**
     * 메시지 키 기반으로 파티션 번호 결정
     */
    fun getPartitionNumber(messageKey: String, partitionCount: Int): Int {
        return abs(messageKey.hashCode()) % partitionCount
    }
    
    /**
     * 큐 이름에 파티션 번호 추가
     */
    fun getPartitionedQueueName(queueName: String, partitionNumber: Int): String {
        return "${queueName}_partition_${partitionNumber}"
    }
    
    /**
     * 파티션된 큐 이름들 생성
     */
    fun getAllPartitionedQueueNames(queueName: String, partitionCount: Int): List<String> {
        return (0 until partitionCount).map { partitionNumber ->
            getPartitionedQueueName(queueName, partitionNumber)
        }
    }
    
    /**
     * 라운드 로빈 방식으로 파티션 선택
     */
    fun getRoundRobinPartition(counter: Long, partitionCount: Int): Int {
        return (counter % partitionCount).toInt()
    }
    
    /**
     * 파티션별 로드 밸런싱을 위한 가중치 계산
     */
    fun calculatePartitionWeights(queueName: String, partitionCount: Int): Map<Int, Double> {
        // 실제로는 Redis에서 각 파티션의 메시지 수를 조회하여 가중치 계산
        // 현재는 균등 분배로 구현
        val weight = 1.0 / partitionCount
        return (0 until partitionCount).associateWith { weight }
    }
}

/**
 * 파티션 정보를 담는 데이터 클래스
 */
data class PartitionInfo(
    val partitionNumber: Int,
    val queueName: String,
    val messageCount: Long = 0,
    val lastAccessTime: Long = System.currentTimeMillis()
)
