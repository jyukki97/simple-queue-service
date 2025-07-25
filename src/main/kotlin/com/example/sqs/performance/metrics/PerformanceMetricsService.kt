package com.example.sqs.performance.metrics

import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * 성능 메트릭 수집 및 모니터링 서비스
 * - 실시간 성능 지표 수집
 * - 병목 지점 식별
 * - 성능 임계값 모니터링
 */
@Service
class PerformanceMetricsService(
    private val meterRegistry: MeterRegistry
) {
    
    private val operationTimers = ConcurrentHashMap<String, Timer>()
    private val operationCounters = ConcurrentHashMap<String, Counter>()
    private val performanceHistory = ConcurrentHashMap<String, MutableList<PerformanceSnapshot>>()
    
    /**
     * 작업 시간 측정 시작
     */
    fun startTimer(operation: String): Timer.Sample {
        return Timer.start(meterRegistry)
    }
    
    /**
     * 작업 시간 측정 완료 및 기록
     */
    fun recordTiming(operation: String, sample: Timer.Sample): Duration {
        val timer = operationTimers.computeIfAbsent(operation) {
            Timer.builder("operation.duration")
                .description("Operation execution time")
                .tag("operation", operation)
                .register(meterRegistry)
        }
        
        sample.stop(timer)
        return timer.totalTime(java.util.concurrent.TimeUnit.MILLISECONDS).let { Duration.ofMillis(it.toLong()) }
    }
    
    /**
     * 작업 횟수 카운터 증가
     */
    fun incrementCounter(operation: String, tags: Map<String, String> = emptyMap()) {
        val counter = operationCounters.computeIfAbsent("$operation-${tags.hashCode()}") {
            var builder = Counter.builder("operation.count")
                .description("Operation execution count")
                .tag("operation", operation)
            
            tags.forEach { (key, value) ->
                builder = builder.tag(key, value)
            }
            
            builder.register(meterRegistry)
        }
        
        counter.increment()
    }
    
    /**
     * 성능 스냅샷 생성
     */
    fun createPerformanceSnapshot(operation: String): Mono<PerformanceSnapshot> {
        return Mono.fromCallable {
            val snapshot = PerformanceSnapshot(
                operation = operation,
                timestamp = Instant.now(),
                totalCount = operationCounters["$operation-${emptyMap<String, String>().hashCode()}"]?.count()?.toLong() ?: 0L,
                averageLatency = operationTimers[operation]?.mean(java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                maxLatency = operationTimers[operation]?.max(java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                p95Latency = operationTimers[operation]?.percentile(0.95, java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                p99Latency = operationTimers[operation]?.percentile(0.99, java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                errorRate = calculateErrorRate(operation),
                throughputPerSecond = calculateThroughput(operation)
            )
            
            // 성능 히스토리 업데이트 (최근 100개만 유지)
            performanceHistory.computeIfAbsent(operation) { mutableListOf() }.let { history ->
                history.add(snapshot)
                if (history.size > 100) {
                    history.removeAt(0)
                }
            }
            
            snapshot
        }
    }
    
    /**
     * 성능 히스토리 조회
     */
    fun getPerformanceHistory(operation: String): List<PerformanceSnapshot> {
        return performanceHistory[operation]?.toList() ?: emptyList()
    }
    
    /**
     * 전체 성능 요약 정보
     */
    fun getPerformanceSummary(): Mono<PerformanceSummary> {
        return Mono.fromCallable {
            val operations = operationTimers.keys.toList()
            val totalOperations = operationCounters.values.sumOf { it.count().toLong() }
            val avgLatency = operationTimers.values.map { it.mean(java.util.concurrent.TimeUnit.MILLISECONDS) }.average()
            val maxLatency = operationTimers.values.maxOfOrNull { it.max(java.util.concurrent.TimeUnit.MILLISECONDS) } ?: 0.0
            
            PerformanceSummary(
                timestamp = Instant.now(),
                totalOperations = totalOperations,
                activeOperationTypes = operations.size,
                averageLatencyMs = avgLatency,
                maxLatencyMs = maxLatency,
                operationBreakdown = operations.associateWith { operation ->
                    OperationMetrics(
                        count = operationCounters["$operation-${emptyMap<String, String>().hashCode()}"]?.count()?.toLong() ?: 0L,
                        avgLatency = operationTimers[operation]?.mean(java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0,
                        maxLatency = operationTimers[operation]?.max(java.util.concurrent.TimeUnit.MILLISECONDS) ?: 0.0
                    )
                }
            )
        }
    }
    
    /**
     * 성능 임계값 위반 검사
     */
    fun checkPerformanceThresholds(): Mono<List<PerformanceAlert>> {
        return Mono.fromCallable {
            val alerts = mutableListOf<PerformanceAlert>()
            
            operationTimers.forEach { (operation, timer) ->
                val avgLatency = timer.mean(java.util.concurrent.TimeUnit.MILLISECONDS)
                val p95Latency = timer.percentile(0.95, java.util.concurrent.TimeUnit.MILLISECONDS)
                
                // 평균 응답시간 임계값 검사 (1초)
                if (avgLatency > 1000.0) {
                    alerts.add(
                        PerformanceAlert(
                            operation = operation,
                            alertType = AlertType.HIGH_LATENCY,
                            threshold = 1000.0,
                            actualValue = avgLatency,
                            severity = if (avgLatency > 5000.0) AlertSeverity.CRITICAL else AlertSeverity.WARNING,
                            timestamp = Instant.now()
                        )
                    )
                }
                
                // P95 응답시간 임계값 검사 (3초)
                if (p95Latency > 3000.0) {
                    alerts.add(
                        PerformanceAlert(
                            operation = operation,
                            alertType = AlertType.HIGH_P95_LATENCY,
                            threshold = 3000.0,
                            actualValue = p95Latency,
                            severity = AlertSeverity.WARNING,
                            timestamp = Instant.now()
                        )
                    )
                }
            }
            
            alerts
        }
    }
    
    private fun calculateErrorRate(operation: String): Double {
        val successCounter = operationCounters["$operation-success"]?.count() ?: 0.0
        val errorCounter = operationCounters["$operation-error"]?.count() ?: 0.0
        val total = successCounter + errorCounter
        
        return if (total > 0) (errorCounter / total) * 100.0 else 0.0
    }
    
    private fun calculateThroughput(operation: String): Double {
        val history = performanceHistory[operation] ?: return 0.0
        if (history.size < 2) return 0.0
        
        val recent = history.takeLast(10)
        val timeSpan = Duration.between(recent.first().timestamp, recent.last().timestamp).toSeconds()
        val countDiff = recent.last().totalCount - recent.first().totalCount
        
        return if (timeSpan > 0) countDiff.toDouble() / timeSpan else 0.0
    }
}

/**
 * 성능 스냅샷 데이터 클래스
 */
data class PerformanceSnapshot(
    val operation: String,
    val timestamp: Instant,
    val totalCount: Long,
    val averageLatency: Double,
    val maxLatency: Double,
    val p95Latency: Double,
    val p99Latency: Double,
    val errorRate: Double,
    val throughputPerSecond: Double
)

/**
 * 성능 요약 정보
 */
data class PerformanceSummary(
    val timestamp: Instant,
    val totalOperations: Long,
    val activeOperationTypes: Int,
    val averageLatencyMs: Double,
    val maxLatencyMs: Double,
    val operationBreakdown: Map<String, OperationMetrics>
)

/**
 * 작업별 메트릭
 */
data class OperationMetrics(
    val count: Long,
    val avgLatency: Double,
    val maxLatency: Double
)

/**
 * 성능 알림
 */
data class PerformanceAlert(
    val operation: String,
    val alertType: AlertType,
    val threshold: Double,
    val actualValue: Double,
    val severity: AlertSeverity,
    val timestamp: Instant
)

enum class AlertType {
    HIGH_LATENCY,
    HIGH_P95_LATENCY,
    HIGH_ERROR_RATE,
    LOW_THROUGHPUT
}

enum class AlertSeverity {
    INFO,
    WARNING,
    CRITICAL
}