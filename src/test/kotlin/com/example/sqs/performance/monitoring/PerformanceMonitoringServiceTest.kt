package com.example.sqs.performance.monitoring

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.simple.SimpleMeterRegistry
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.boot.test.context.SpringBootTest
import kotlin.test.assertEquals
import kotlin.test.assertTrue

@SpringBootTest
class PerformanceMonitoringServiceTest {
    
    private lateinit var meterRegistry: MeterRegistry
    private lateinit var monitoringService: PerformanceMonitoringService
    
    @BeforeEach
    fun setUp() {
        meterRegistry = SimpleMeterRegistry()
        monitoringService = PerformanceMonitoringService(meterRegistry)
    }
    
    @Test
    fun `작업 실행 시간이 측정되어야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val operationName = "sendMessage"
        
        val result = monitoringService.measureOperationSync(operationName, tenantId, queueName) {
            Thread.sleep(100) // 100ms 대기
            "test-result"
        }
        
        assertEquals("test-result", result)
        
        // 타이머가 생성되었는지 확인
        val timer = meterRegistry.find("sqs.operation.duration")
            .tag("operation", operationName)
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .timer()
        
        assertTrue(timer != null)
        assertEquals(1L, timer.count())
        assertTrue(timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS) >= 100)
    }
    
    @Test
    fun `카운터가 올바르게 증가해야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val operationName = "receiveMessage"
        
        // 카운터를 3번 증가
        repeat(3) {
            monitoringService.incrementCounter(operationName, tenantId, queueName)
        }
        
        val counter = meterRegistry.find("sqs.operation.count")
            .tag("operation", operationName)
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .counter()
        
        assertTrue(counter != null)
        assertEquals(3.0, counter.count())
    }
    
    @Test
    fun `에러 카운터가 올바르게 증가해야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val operationName = "deleteMessage"
        val errorType = "MessageNotFound"
        
        monitoringService.incrementErrorCounter(operationName, tenantId, errorType, queueName)
        
        val errorCounter = meterRegistry.find("sqs.operation.error")
            .tag("operation", operationName)
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .tag("error_type", errorType)
            .counter()
        
        assertTrue(errorCounter != null)
        assertEquals(1.0, errorCounter.count())
    }
    
    @Test
    fun `메시지 크기가 기록되어야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val messageSize = 1024
        
        monitoringService.recordMessageSize(tenantId, queueName, messageSize)
        
        val summary = meterRegistry.find("sqs.message.size")
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .summary()
        
        assertTrue(summary != null)
        assertEquals(1L, summary.count())
        assertEquals(1024.0, summary.totalAmount())
    }
    
    @Test
    fun `큐 깊이가 기록되어야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val depth = 150L
        
        monitoringService.recordQueueDepth(tenantId, queueName, depth)
        
        val gauge = meterRegistry.find("sqs.queue.depth")
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .gauge()
        
        assertTrue(gauge != null)
        assertEquals(150.0, gauge.value())
    }
    
    @Test
    fun `Redis 연결 풀 상태가 기록되어야 한다`() {
        val activeConnections = 8
        val idleConnections = 2
        val maxConnections = 10
        
        monitoringService.recordRedisPoolStats(activeConnections, idleConnections, maxConnections)
        
        val activeGauge = meterRegistry.find("sqs.redis.pool.active").gauge()
        val idleGauge = meterRegistry.find("sqs.redis.pool.idle").gauge()
        val maxGauge = meterRegistry.find("sqs.redis.pool.max").gauge()
        val utilizationGauge = meterRegistry.find("sqs.redis.pool.utilization").gauge()
        
        assertTrue(activeGauge != null)
        assertTrue(idleGauge != null)
        assertTrue(maxGauge != null)
        assertTrue(utilizationGauge != null)
        
        assertEquals(8.0, activeGauge.value())
        assertEquals(2.0, idleGauge.value())
        assertEquals(10.0, maxGauge.value())
        assertEquals(0.8, utilizationGauge.value()) // 8/10 = 0.8
    }
    
    @Test
    fun `메모리 사용량이 기록되어야 한다`() {
        monitoringService.recordMemoryUsage()
        
        val usedGauge = meterRegistry.find("sqs.memory.used").gauge()
        val freeGauge = meterRegistry.find("sqs.memory.free").gauge()
        val totalGauge = meterRegistry.find("sqs.memory.total").gauge()
        val maxGauge = meterRegistry.find("sqs.memory.max").gauge()
        val utilizationGauge = meterRegistry.find("sqs.memory.utilization").gauge()
        
        assertTrue(usedGauge != null)
        assertTrue(freeGauge != null)
        assertTrue(totalGauge != null)
        assertTrue(maxGauge != null)
        assertTrue(utilizationGauge != null)
        
        // 메모리 값들이 양수여야 함
        assertTrue(usedGauge.value() >= 0)
        assertTrue(freeGauge.value() >= 0)
        assertTrue(totalGauge.value() > 0)
        assertTrue(maxGauge.value() > 0)
        assertTrue(utilizationGauge.value() >= 0)
        assertTrue(utilizationGauge.value() <= 1)
    }
    
    @Test
    fun `압축률이 기록되어야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        val compressionRatio = 75.5
        
        monitoringService.recordCompressionRatio(tenantId, queueName, compressionRatio)
        
        val summary = meterRegistry.find("sqs.compression.ratio")
            .tag("tenant", tenantId)
            .tag("queue", queueName)
            .summary()
        
        assertTrue(summary != null)
        assertEquals(1L, summary.count())
        assertEquals(75.5, summary.totalAmount())
    }
    
    @Test
    fun `배치 처리 효율성이 기록되어야 한다`() {
        val tenantId = "test-tenant"
        val operationType = "sendMessage"
        val batchSize = 10
        val processingTimeMs = 500L
        
        monitoringService.recordBatchEfficiency(tenantId, operationType, batchSize, processingTimeMs)
        
        val sizeSummary = meterRegistry.find("sqs.batch.size")
            .tag("tenant", tenantId)
            .tag("operation", operationType)
            .summary()
        
        val timeSummary = meterRegistry.find("sqs.batch.processing_time")
            .tag("tenant", tenantId)
            .tag("operation", operationType)
            .summary()
        
        val throughputGauge = meterRegistry.find("sqs.batch.throughput")
            .tag("tenant", tenantId)
            .tag("operation", operationType)
            .gauge()
        
        assertTrue(sizeSummary != null)
        assertTrue(timeSummary != null)
        assertTrue(throughputGauge != null)
        
        assertEquals(10.0, sizeSummary.totalAmount())
        assertEquals(500.0, timeSummary.totalAmount())
        assertEquals(20.0, throughputGauge.value(), 0.1) // 10 messages / 0.5 seconds = 20 msg/sec
    }
    
    @Test
    fun `처리 시간이 0일 때 배치 처리율이 0이어야 한다`() {
        val tenantId = "test-tenant"
        val operationType = "sendMessage"
        val batchSize = 5
        val processingTimeMs = 0L
        
        monitoringService.recordBatchEfficiency(tenantId, operationType, batchSize, processingTimeMs)
        
        val throughputGauge = meterRegistry.find("sqs.batch.throughput")
            .tag("tenant", tenantId)
            .tag("operation", operationType)
            .gauge()
        
        assertTrue(throughputGauge != null)
        assertEquals(0.0, throughputGauge.value())
    }
    
    @Test
    fun `성능 통계가 올바르게 조회되어야 한다`() {
        val tenantId = "test-tenant"
        val queueName = "test-queue"
        
        // 몇 가지 카운터 증가
        monitoringService.incrementCounter("sendMessage", tenantId, queueName)
        monitoringService.incrementCounter("receiveMessage", tenantId, queueName)
        monitoringService.incrementCounter("sendMessage", tenantId, queueName)
        
        val stats = monitoringService.getPerformanceStats(tenantId)
        
        assertEquals(tenantId, stats.tenantId)
        assertTrue(stats.operationCounts.isNotEmpty())
        assertTrue(stats.timestamp.epochSecond > 0)
    }
}
