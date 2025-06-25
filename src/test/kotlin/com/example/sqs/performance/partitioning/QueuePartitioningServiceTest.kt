package com.example.sqs.performance.partitioning

import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = [
    "app.metrics.enable-prometheus=false"
])
class QueuePartitioningServiceTest {
    
    private val partitioningService = QueuePartitioningService()
    
    @Test
    fun `같은 메시지 키는 항상 같은 파티션을 반환해야 한다`() {
        val messageKey = "test-message-key"
        val partitionCount = 4
        
        val partition1 = partitioningService.getPartitionNumber(messageKey, partitionCount)
        val partition2 = partitioningService.getPartitionNumber(messageKey, partitionCount)
        
        assertEquals(partition1, partition2)
    }
    
    @Test
    fun `파티션 번호는 0부터 partitionCount-1 범위 내에 있어야 한다`() {
        val partitionCount = 8
        val testKeys = listOf(
            "key1", "key2", "key3", "another-key", 
            "한글키", "🚀emoji-key", "", "very-long-key-name-for-testing"
        )
        
        testKeys.forEach { key ->
            val partitionNumber = partitioningService.getPartitionNumber(key, partitionCount)
            assertTrue(partitionNumber >= 0, "파티션 번호는 0 이상이어야 합니다")
            assertTrue(partitionNumber < partitionCount, "파티션 번호는 $partitionCount 미만이어야 합니다")
        }
    }
    
    @Test
    fun `파티션된 큐 이름이 올바르게 생성되어야 한다`() {
        val queueName = "test-queue"
        val partitionNumber = 2
        
        val partitionedName = partitioningService.getPartitionedQueueName(queueName, partitionNumber)
        
        assertEquals("test-queue_partition_2", partitionedName)
    }
    
    @Test
    fun `모든 파티션된 큐 이름들이 올바르게 생성되어야 한다`() {
        val queueName = "my-queue"
        val partitionCount = 3
        
        val partitionedNames = partitioningService.getAllPartitionedQueueNames(queueName, partitionCount)
        
        assertEquals(3, partitionedNames.size)
        assertEquals("my-queue_partition_0", partitionedNames[0])
        assertEquals("my-queue_partition_1", partitionedNames[1])
        assertEquals("my-queue_partition_2", partitionedNames[2])
    }
    
    @Test
    fun `라운드 로빈 파티션 선택이 올바르게 작동해야 한다`() {
        val partitionCount = 4
        
        val partitions = (0L..15L).map { counter ->
            partitioningService.getRoundRobinPartition(counter, partitionCount)
        }
        
        // 0, 1, 2, 3, 0, 1, 2, 3, ... 패턴이어야 함
        assertEquals(listOf(0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3, 0, 1, 2, 3), partitions)
    }
    
    @Test
    fun `파티션 가중치가 균등하게 계산되어야 한다`() {
        val queueName = "test-queue"
        val partitionCount = 4
        
        val weights = partitioningService.calculatePartitionWeights(queueName, partitionCount)
        
        assertEquals(4, weights.size)
        weights.values.forEach { weight ->
            assertEquals(0.25, weight, 0.001)
        }
    }
    
    @Test
    fun `빈 문자열 키도 올바르게 처리되어야 한다`() {
        val emptyKey = ""
        val partitionCount = 5
        
        val partitionNumber = partitioningService.getPartitionNumber(emptyKey, partitionCount)
        
        assertTrue(partitionNumber >= 0)
        assertTrue(partitionNumber < partitionCount)
    }
    
    @Test
    fun `다양한 키 분포가 균등해야 한다`() {
        val partitionCount = 4
        val testKeys = (1..1000).map { "key-$it" }
        
        val partitionCounts = mutableMapOf<Int, Int>()
        
        testKeys.forEach { key ->
            val partition = partitioningService.getPartitionNumber(key, partitionCount)
            partitionCounts[partition] = partitionCounts.getOrDefault(partition, 0) + 1
        }
        
        // 각 파티션에 최소한의 키가 분배되어야 함
        assertEquals(4, partitionCounts.size)
        partitionCounts.values.forEach { count ->
            assertTrue(count > 200, "각 파티션에 적어도 200개 이상의 키가 분배되어야 합니다")
            assertTrue(count < 300, "키 분배가 너무 편중되어서는 안 됩니다")
        }
    }
    
    @Test
    fun `유니코드 키가 올바르게 처리되어야 한다`() {
        val unicodeKeys = listOf(
            "한글테스트키",
            "🚀🎉🔥",
            "中文测试键",
            "العربية",
            "🇰🇷🇺🇸🇯🇵"
        )
        val partitionCount = 3
        
        unicodeKeys.forEach { key ->
            val partitionNumber = partitioningService.getPartitionNumber(key, partitionCount)
            assertTrue(partitionNumber >= 0)
            assertTrue(partitionNumber < partitionCount)
        }
    }
    
    @Test
    fun `파티션 카운트가 1일 때 항상 0을 반환해야 한다`() {
        val testKeys = listOf("key1", "key2", "key3")
        val partitionCount = 1
        
        testKeys.forEach { key ->
            val partitionNumber = partitioningService.getPartitionNumber(key, partitionCount)
            assertEquals(0, partitionNumber)
        }
    }
}
