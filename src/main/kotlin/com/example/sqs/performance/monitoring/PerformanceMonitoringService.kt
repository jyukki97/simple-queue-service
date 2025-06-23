package com.example.sqs.performance.monitoring

import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Component
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong

/**
 * 성능 모니터링 서비스
 */
@Component
class PerformanceMonitoringService(
    private val meterRegistry: MeterRegistry
) {
    
    private val operationCounters = ConcurrentHashMap<String, AtomicLong>()
    private val operationTimers = ConcurrentHashMap<String, Timer>()
    
    /**
     * 작업 실행 시간 측정 (일반 함수)
     */
    fun <T> measureOperationSync(
        operationName: String,
        tenantId: String,
        queueName: String? = null,
        operation: () -> T
    ): T {
        val timer = getOrCreateTimer(operationName, tenantId, queueName)
        val startTime = System.nanoTime()
        return try {
            operation()
        } finally {
            val duration = Duration.ofNanos(System.nanoTime() - startTime)
            timer.record(duration)
        }
    }
    
    /**
     * 작업 카운터 증가
     */
    fun incrementCounter(operationName: String, tenantId: String, queueName: String? = null) {
        val key = createMetricKey(operationName, tenantId, queueName)
        operationCounters.computeIfAbsent(key) { AtomicLong(0) }.incrementAndGet()
        
        // Micrometer 카운터 업데이트
        meterRegistry.counter(
            "sqs.operation.count",
            "operation", operationName,
            "tenant", tenantId,
            "queue", queueName ?: "all"
        ).increment()
    }
    
    /**
     * 에러 카운터 증가
     */
    fun incrementErrorCounter(
        operationName: String,
        tenantId: String,
        errorType: String,
        queueName: String? = null
    ) {
        meterRegistry.counter(
            "sqs.operation.error",
            "operation", operationName,
            "tenant", tenantId,
            "queue", queueName ?: "all",
            "error_type", errorType
        ).increment()
    }
    
    /**
     * 메시지 크기 기록
     */
    fun recordMessageSize(tenantId: String, queueName: String, size: Int) {
        meterRegistry.summary(
            "sqs.message.size",
            "tenant", tenantId,
            "queue", queueName
        ).record(size.toDouble())
    }
    
    /**
     * 큐 깊이 기록
     */
    fun recordQueueDepth(tenantId: String, queueName: String, depth: Long) {
        meterRegistry.gauge(
            "sqs.queue.depth",
            listOf(
                io.micrometer.core.instrument.Tag.of("tenant", tenantId),
                io.micrometer.core.instrument.Tag.of("queue", queueName)
            ),
            depth.toDouble()
        )
    }
    
    /**
     * Redis 연결 풀 상태 모니터링
     */
    fun recordRedisPoolStats(activeConnections: Int, idleConnections: Int, maxConnections: Int) {
        meterRegistry.gauge("sqs.redis.pool.active", activeConnections.toDouble())
        meterRegistry.gauge("sqs.redis.pool.idle", idleConnections.toDouble())
        meterRegistry.gauge("sqs.redis.pool.max", maxConnections.toDouble())
        meterRegistry.gauge("sqs.redis.pool.utilization", 
            activeConnections.toDouble() / maxConnections)
    }
    
    /**
     * 메모리 사용량 모니터링
     */
    fun recordMemoryUsage() {
        val runtime = Runtime.getRuntime()
        val totalMemory = runtime.totalMemory()
        val freeMemory = runtime.freeMemory()
        val usedMemory = totalMemory - freeMemory
        val maxMemory = runtime.maxMemory()
        
        meterRegistry.gauge("sqs.memory.used", usedMemory.toDouble())
        meterRegistry.gauge("sqs.memory.free", freeMemory.toDouble())
        meterRegistry.gauge("sqs.memory.total", totalMemory.toDouble())
        meterRegistry.gauge("sqs.memory.max", maxMemory.toDouble())
        meterRegistry.gauge("sqs.memory.utilization", usedMemory.toDouble() / maxMemory)
    }
    
    /**
     * 압축률 모니터링
     */
    fun recordCompressionRatio(tenantId: String, queueName: String, ratio: Double) {
        meterRegistry.summary(
            "sqs.compression.ratio",
            "tenant", tenantId,
            "queue", queueName
        ).record(ratio)
    }
    
    /**
     * 배치 처리 효율성 모니터링
     */
    fun recordBatchEfficiency(
        tenantId: String,
        operationType: String,
        batchSize: Int,
        processingTimeMs: Long
    ) {
        meterRegistry.summary(
            "sqs.batch.size",
            "tenant", tenantId,
            "operation", operationType
        ).record(batchSize.toDouble())
        
        meterRegistry.summary(
            "sqs.batch.processing_time",
            "tenant", tenantId,
            "operation", operationType
        ).record(processingTimeMs.toDouble())
        
        // 처리량 계산 (messages per second)
        val throughput = if (processingTimeMs > 0) {
            batchSize.toDouble() / (processingTimeMs / 1000.0)
        } else {
            0.0
        }
        
        meterRegistry.gauge(
            "sqs.batch.throughput",
            listOf(
                io.micrometer.core.instrument.Tag.of("tenant", tenantId),
                io.micrometer.core.instrument.Tag.of("operation", operationType)
            ),
            throughput
        )
    }
    
    /**
     * 성능 통계 조회
     */
    fun getPerformanceStats(tenantId: String): PerformanceStats {
        val operationCounts = operationCounters.entries
            .filter { it.key.contains(tenantId) }
            .associate { (key, value) -> 
                key.substringAfter("${tenantId}:") to value.get() 
            }
        
        return PerformanceStats(
            tenantId = tenantId,
            operationCounts = operationCounts,
            timestamp = Instant.now()
        )
    }
    
    private fun getOrCreateTimer(
        operationName: String,
        tenantId: String,
        queueName: String?
    ): Timer {
        val key = createMetricKey(operationName, tenantId, queueName)
        return operationTimers.computeIfAbsent(key) {
            Timer.builder("sqs.operation.duration")
                .tag("operation", operationName)
                .tag("tenant", tenantId)
                .tag("queue", queueName ?: "all")
                .register(meterRegistry)
        }
    }
    
    private fun createMetricKey(
        operationName: String,
        tenantId: String,
        queueName: String?
    ): String {
        return "${tenantId}:${operationName}:${queueName ?: "all"}"
    }
    
    /**
     * 압축 비율을 계산합니다
     */
    fun calculateCompressionRatio(originalSize: Int, compressedSize: Int): Double {
        return if (originalSize > 0) {
            (originalSize - compressedSize).toDouble() / originalSize * 100
        } else {
            0.0
        }
    }

    /**
     * 배치 처리 시간을 기록합니다
     */
    fun recordBatchProcessingTime(tenantId: String, queueName: String, processingTimeMs: Long) {
        meterRegistry.timer(
            "sqs.batch.processing.time",
            "tenant", tenantId,
            "queue", queueName
        ).record(Duration.ofMillis(processingTimeMs))
    }

    /**
     * 배치 처리 결과를 기록합니다
     */
    fun recordBatchProcessingResults(tenantId: String, queueName: String, successCount: Int, failedCount: Int) {
        meterRegistry.counter(
            "sqs.batch.messages.successful",
            "tenant", tenantId,
            "queue", queueName
        ).increment(successCount.toDouble())

        meterRegistry.counter(
            "sqs.batch.messages.failed",
            "tenant", tenantId,
            "queue", queueName
        ).increment(failedCount.toDouble())
    }

    // ...existing code...
}

/**
 * 성능 통계 데이터 클래스
 */
data class PerformanceStats(
    val tenantId: String,
    val operationCounts: Map<String, Long>,
    val timestamp: Instant
)
