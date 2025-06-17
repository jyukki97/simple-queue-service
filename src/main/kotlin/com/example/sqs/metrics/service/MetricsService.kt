package com.example.sqs.metrics.service

import com.example.sqs.metrics.domain.*
import com.example.sqs.queue.repository.QueueRepository
import com.example.sqs.message.repository.MessageRepository
import io.micrometer.core.instrument.Counter
import io.micrometer.core.instrument.Gauge
import io.micrometer.core.instrument.MeterRegistry
import io.micrometer.core.instrument.Timer
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Duration
import java.time.Instant
import java.time.temporal.ChronoUnit
import java.util.concurrent.ConcurrentHashMap
import java.util.concurrent.atomic.AtomicLong
import java.util.concurrent.atomic.AtomicReference
import kotlin.random.Random

/**
 * 실시간 메트릭 수집 및 관리 서비스
 */
@Service
class MetricsService(
    private val queueRepository: QueueRepository,
    private val messageRepository: MessageRepository,
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val meterRegistry: MeterRegistry
) {
    
    // 메트릭 카운터 및 게이지
    private val messagesSentCounter = Counter.builder("sqs.messages.sent.total")
        .description("전송된 메시지 총 수")
        .register(meterRegistry)
    
    private val messagesReceivedCounter = Counter.builder("sqs.messages.received.total")
        .description("수신된 메시지 총 수")
        .register(meterRegistry)
    
    private val messagesDeletedCounter = Counter.builder("sqs.messages.deleted.total")
        .description("삭제된 메시지 총 수")
        .register(meterRegistry)
    
    private val messagesFailedCounter = Counter.builder("sqs.messages.failed.total")
        .description("실패한 메시지 총 수")
        .register(meterRegistry)
    
    private val processingTimer = Timer.builder("sqs.message.processing.time")
        .description("메시지 처리 시간")
        .register(meterRegistry)
    
    // 현재 메트릭 상태 캐시
    private val queueMetricsCache = ConcurrentHashMap<String, QueueMetrics>()
    private val systemMetricsCache = AtomicReference<SystemMetrics>()
    
    // 시계열 데이터 저장소 (실제로는 별도 TSDB 사용 권장)
    private val timeSeriesData = ConcurrentHashMap<String, MutableList<MetricDataPoint>>()
    
    /**
     * 메시지 전송 메트릭 기록
     */
    fun recordMessageSent(tenantId: String, queueName: String, messageSize: Int) {
        messagesSentCounter.increment()
        recordTimeSeries("messages_sent_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ), 1.0)
        
        recordTimeSeries("message_size", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ), messageSize.toDouble())
    }
    
    /**
     * 메시지 수신 메트릭 기록
     */
    fun recordMessageReceived(tenantId: String, queueName: String, processingTimeMs: Long) {
        messagesReceivedCounter.increment()
        processingTimer.record(Duration.ofMillis(processingTimeMs))
        
        recordTimeSeries("messages_received_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ), 1.0)
        
        recordTimeSeries("processing_time", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ), processingTimeMs.toDouble())
    }
    
    /**
     * 메시지 삭제 메트릭 기록
     */
    fun recordMessageDeleted(tenantId: String, queueName: String) {
        messagesDeletedCounter.increment()
        recordTimeSeries("messages_deleted_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ), 1.0)
    }
    
    /**
     * 메시지 실패 메트릭 기록
     */
    fun recordMessageFailed(tenantId: String, queueName: String, errorType: String) {
        messagesFailedCounter.increment()
        recordTimeSeries("messages_failed_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName,
            "error_type" to errorType
        ), 1.0)
    }
    
    /**
     * 큐별 메트릭 수집
     */
    fun collectQueueMetrics(tenantId: String, queueName: String): Mono<QueueMetrics> {
        return Mono.fromCallable {
            val timestamp = Instant.now()
            
            // 실제 환경에서는 Redis에서 실시간 데이터를 조회
            val metrics = QueueMetrics(
                queueName = queueName,
                tenantId = tenantId,
                timestamp = timestamp,
                messagesAvailable = getRandomMetric(0, 1000),
                messagesInFlight = getRandomMetric(0, 100),
                messagesDelayed = getRandomMetric(0, 50),
                messagesInDlq = getRandomMetric(0, 10),
                messagesSentPerMinute = getRandomMetric(0, 100).toDouble(),
                messagesReceivedPerMinute = getRandomMetric(0, 100).toDouble(),
                messagesDeletedPerMinute = getRandomMetric(0, 100).toDouble(),
                messagesFailedPerMinute = getRandomMetric(0, 10).toDouble(),
                averageAgeOfOldestMessage = getRandomMetric(0, 3600),
                averageProcessingTime = getRandomMetric(100, 5000).toDouble(),
                errorRate = Random.nextDouble(0.0, 5.0),
                totalMessageSize = getRandomMetric(1024, 1048576),
                averageMessageSize = getRandomMetric(1024, 10240).toDouble()
            )
            
            queueMetricsCache[getQueueKey(tenantId, queueName)] = metrics
            
            // Prometheus 게이지 업데이트
            updatePrometheusGauges(metrics)
            
            metrics
        }
    }
    
    /**
     * 시스템 전체 메트릭 수집
     */
    fun collectSystemMetrics(): Mono<SystemMetrics> {
        return Mono.fromCallable {
            val timestamp = Instant.now()
            
            val metrics = SystemMetrics(
                timestamp = timestamp,
                totalQueues = getRandomMetric(10, 100),
                totalMessages = getRandomMetric(1000, 10000),
                totalTenants = getRandomMetric(5, 50),
                totalThroughputPerSecond = getRandomMetric(100, 1000).toDouble(),
                averageLatency = getRandomMetric(50, 500).toDouble(),
                errorRate = Random.nextDouble(0.0, 2.0),
                memoryUsageBytes = getRandomMetric(100_000_000, 1_000_000_000),
                cpuUsagePercent = Random.nextDouble(10.0, 80.0),
                redisConnectionsActive = getRandomMetric(5, 20).toInt(),
                redisConnectionsIdle = getRandomMetric(0, 10).toInt()
            )
            
            systemMetricsCache.set(metrics)
            
            // Prometheus 게이지 업데이트
            updateSystemPrometheusGauges(metrics)
            
            metrics
        }
    }
    
    /**
     * 시계열 메트릭 조회
     */
    fun getTimeSeriesMetrics(
        metricName: String,
        labels: Map<String, String>,
        startTime: Instant,
        endTime: Instant
    ): Mono<MetricTimeSeries> {
        return Mono.fromCallable {
            val key = buildTimeSeriesKey(metricName, labels)
            val dataPoints = timeSeriesData[key]?.filter { 
                it.timestamp.isAfter(startTime) && it.timestamp.isBefore(endTime)
            } ?: emptyList()
            
            MetricTimeSeries(
                metricName = metricName,
                labels = labels,
                dataPoints = dataPoints
            )
        }
    }
    
    /**
     * 모든 큐의 메트릭 조회
     */
    fun getAllQueueMetrics(tenantId: String): Flux<QueueMetrics> {
        return Flux.fromIterable(listOf("queue1", "queue2", "queue3")) // 임시로 하드코딩
            .flatMap { queueName ->
                collectQueueMetrics(tenantId, queueName)
            }
    }
    
    /**
     * 캐시된 메트릭 조회
     */
    fun getCachedQueueMetrics(tenantId: String, queueName: String): QueueMetrics? {
        return queueMetricsCache[getQueueKey(tenantId, queueName)]
    }
    
    fun getCachedSystemMetrics(): SystemMetrics? {
        return systemMetricsCache.get()
    }
    
    private fun recordTimeSeries(metricName: String, labels: Map<String, String>, value: Double) {
        val key = buildTimeSeriesKey(metricName, labels)
        val dataPoint = MetricDataPoint(Instant.now(), value)
        
        timeSeriesData.computeIfAbsent(key) { mutableListOf() }.add(dataPoint)
        
        // 오래된 데이터 정리 (최근 24시간만 유지)
        val cutoffTime = Instant.now().minus(24, ChronoUnit.HOURS)
        timeSeriesData[key]?.removeIf { it.timestamp.isBefore(cutoffTime) }
    }
    
    private fun updatePrometheusGauges(metrics: QueueMetrics) {
        // 간단히 메트릭만 수집하고 Prometheus 게이지는 나중에 구현
        // 실제로는 AtomicReference나 다른 방식으로 최신 값 저장
    }
    
    private fun updateSystemPrometheusGauges(metrics: SystemMetrics) {
        // 간단히 메트릭만 수집하고 Prometheus 게이지는 나중에 구현
    }
    
    private fun getQueueKey(tenantId: String, queueName: String) = "$tenantId:$queueName"
    
    private fun buildTimeSeriesKey(metricName: String, labels: Map<String, String>): String {
        val labelString = labels.entries.sortedBy { it.key }
            .joinToString(",") { "${it.key}=${it.value}" }
        return "$metricName{$labelString}"
    }
    
    private fun getRandomMetric(min: Long, max: Long): Long {
        return Random.nextLong(min, max + 1)
    }
}
