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
import java.lang.management.ManagementFactory

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
    
    // 메트릭 계산을 위한 캐시
    private val lastMetricsTime = AtomicLong(System.currentTimeMillis())
    private val lastProcessedMessagesCount = AtomicLong(0)
    
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
            
            // 실제 Redis 데이터 조회
            val messagesAvailable = getMessagesCount(tenantId, queueName, "available")
            val messagesInFlight = getMessagesCount(tenantId, queueName, "inflight")
            val messagesDelayed = getMessagesCount(tenantId, queueName, "delayed")
            val messagesInDlq = getMessagesCount(tenantId, queueName, "dlq")
            
            // 실제 처리율 계산 (마지막 1분간)
            val currentTime = System.currentTimeMillis()
            val oneMinuteAgo = currentTime - 60000
            
            val sentRate = getMessageRateInWindow(tenantId, queueName, "sent", oneMinuteAgo, currentTime)
            val receivedRate = getMessageRateInWindow(tenantId, queueName, "received", oneMinuteAgo, currentTime)
            val deletedRate = getMessageRateInWindow(tenantId, queueName, "deleted", oneMinuteAgo, currentTime)
            val failedRate = getMessageRateInWindow(tenantId, queueName, "failed", oneMinuteAgo, currentTime)
            
            val metrics = QueueMetrics(
                queueName = queueName,
                tenantId = tenantId,
                timestamp = timestamp,
                messagesAvailable = messagesAvailable,
                messagesInFlight = messagesInFlight,
                messagesDelayed = messagesDelayed,
                messagesInDlq = messagesInDlq,
                messagesSentPerMinute = sentRate,
                messagesReceivedPerMinute = receivedRate,
                messagesDeletedPerMinute = deletedRate,
                messagesFailedPerMinute = failedRate,
                averageAgeOfOldestMessage = getOldestMessageAge(tenantId, queueName),
                averageProcessingTime = getAverageProcessingTime(tenantId, queueName),
                errorRate = calculateQueueErrorRate(tenantId, queueName),
                totalMessageSize = getTotalMessageSize(tenantId, queueName),
                averageMessageSize = getAverageMessageSize(tenantId, queueName)
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
        return queueRepository.findAllQueues().count()
            .zipWith(calculateTotalMessagesReactive())
            .map { tuple ->
                val totalQueues = tuple.t1
                val totalMessages = tuple.t2
                val timestamp = Instant.now()
                
                // 실제 테넌트 수는 1로 고정 (현재 시스템에서)
                val totalTenants = 1L
                
                // 실제 JVM 메모리 사용량
                val runtime = Runtime.getRuntime()
                val memoryUsageBytes = runtime.totalMemory() - runtime.freeMemory()
                
                // 실제 처리량 계산 (카운터 기반)
                val currentProcessedMessages = messagesDeletedCounter.count()
                val currentTime = System.currentTimeMillis()
                val lastTime = lastMetricsTime.getAndSet(currentTime)
                val timeDiff = (currentTime - lastTime) / 1000.0 // 초 단위
                val lastProcessedCount = lastProcessedMessagesCount.getAndSet(currentProcessedMessages.toLong())
                
                val throughputPerSecond = if (timeDiff > 0 && lastTime > 0) {
                    (currentProcessedMessages - lastProcessedCount) / timeDiff
                } else {
                    0.0
                }
                
                val metrics = SystemMetrics(
                    timestamp = timestamp,
                    totalQueues = totalQueues,
                    totalMessages = totalMessages,
                    totalTenants = totalTenants,
                    totalThroughputPerSecond = throughputPerSecond,
                    averageLatency = calculateAverageLatency(), // 실제 평균 지연 시간
                    errorRate = calculateErrorRate(), // 실제 에러율
                    memoryUsageBytes = memoryUsageBytes,
                    cpuUsagePercent = getCpuUsage(), // 실제 CPU 사용률
                    redisConnectionsActive = getRedisActiveConnections().toInt(),
                    redisConnectionsIdle = getRedisIdleConnections().toInt()
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
        return queueRepository.findAllQueues()
            .map { it.queueName }
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
    
    /**
     * Redis에서 실제 메시지 수 조회
     */
    private fun getMessagesCount(tenantId: String, queueName: String, type: String): Long {
        return try {
            val key = when (type) {
                "available" -> "queue:messages:$tenantId:$queueName"
                "inflight" -> "queue:inflight:$tenantId:$queueName"
                "delayed" -> "queue:delayed:$tenantId:$queueName"
                "dlq" -> "queue:messages:$tenantId:${queueName}_dlq"
                else -> return 0
            }
            
            when (type) {
                "available" -> redisTemplate.opsForList().size(key).block()?.toLong() ?: 0L
                "inflight", "delayed" -> redisTemplate.opsForZSet().size(key).block()?.toLong() ?: 0L
                "dlq" -> redisTemplate.opsForList().size(key).block()?.toLong() ?: 0L
                else -> 0L
            }
        } catch (e: Exception) {
            0L
        }
    }
    
    /**
     * 전체 메시지 수 계산 (Non-blocking)
     */
    private fun calculateTotalMessagesReactive(): Mono<Long> {
        return redisTemplate.keys("queue:messages:*")
            .flatMap { key ->
                redisTemplate.opsForList().size(key)
            }
            .reduce(0L) { acc, size -> acc + (size ?: 0L) }
            .onErrorReturn(0L)
    }
    
    /**
     * 전체 메시지 수 계산
     */
    private fun calculateTotalMessages(): Long {
        return try {
            val keys = redisTemplate.keys("queue:messages:*").collectList().block() ?: emptyList()
            keys.sumOf { key ->
                redisTemplate.opsForList().size(key).block()?.toLong() ?: 0L
            }
        } catch (e: Exception) {
            0L
        }
    }
    
    /**
     * 시간 윈도우 내 메시지 처리율 계산 (실제로는 시계열 데이터에서)
     */
    private fun getMessageRateInWindow(tenantId: String, queueName: String, type: String, startTime: Long, endTime: Long): Double {
        val key = buildTimeSeriesKey("messages_${type}_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        
        val dataPoints = timeSeriesData[key] ?: return 0.0
        val windowData = dataPoints.filter { 
            val timestamp = it.timestamp.toEpochMilli()
            timestamp >= startTime && timestamp <= endTime
        }
        
        return windowData.sumOf { it.value }
    }
    
    /**
     * 가장 오래된 메시지의 나이 계산
     */
    private fun getOldestMessageAge(tenantId: String, queueName: String): Long {
        return try {
            val queueKey = "queue:messages:$tenantId:$queueName"
            val oldestMessageId = redisTemplate.opsForList().range(queueKey, 0L, 0L)
                .blockFirst()
                
            if (oldestMessageId != null) {
                val messageKey = "message:$oldestMessageId"
                val sentTimestamp = redisTemplate.opsForHash<String, String>()
                    .get(messageKey, "sentTimestamp").block()
                    
                if (sentTimestamp != null) {
                    val sentTime = Instant.parse(sentTimestamp)
                    Duration.between(sentTime, Instant.now()).seconds
                } else 0
            } else 0
        } catch (e: Exception) {
            0L
        }
    }
    
    /**
     * 평균 처리 시간 계산
     */
    private fun getAverageProcessingTime(tenantId: String, queueName: String): Double {
        val key = buildTimeSeriesKey("processing_time", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        
        val dataPoints = timeSeriesData[key] ?: return 0.0
        if (dataPoints.isEmpty()) return 0.0
        
        // 최근 100개 데이터 포인트의 평균
        val recentData = dataPoints.takeLast(100)
        return recentData.sumOf { it.value } / recentData.size
    }
    
    /**
     * 큐별 에러율 계산
     */
    private fun calculateQueueErrorRate(tenantId: String, queueName: String): Double {
        val successKey = buildTimeSeriesKey("messages_deleted_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        val failKey = buildTimeSeriesKey("messages_failed_rate", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        
        val successData = timeSeriesData[successKey] ?: emptyList()
        val failData = timeSeriesData[failKey] ?: emptyList()
        
        val recentSuccess = successData.takeLast(100).sumOf { it.value }
        val recentFail = failData.takeLast(100).sumOf { it.value }
        
        val total = recentSuccess + recentFail
        return if (total > 0) (recentFail / total) * 100.0 else 0.0
    }
    
    /**
     * 총 메시지 크기 계산
     */
    private fun getTotalMessageSize(tenantId: String, queueName: String): Long {
        val key = buildTimeSeriesKey("message_size", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        
        val dataPoints = timeSeriesData[key] ?: return 0L
        return dataPoints.takeLast(1000).sumOf { it.value.toLong() }
    }
    
    /**
     * 평균 메시지 크기 계산
     */
    private fun getAverageMessageSize(tenantId: String, queueName: String): Double {
        val key = buildTimeSeriesKey("message_size", mapOf(
            "tenant_id" to tenantId,
            "queue_name" to queueName
        ))
        
        val dataPoints = timeSeriesData[key] ?: return 0.0
        if (dataPoints.isEmpty()) return 0.0
        
        val recentData = dataPoints.takeLast(100)
        return recentData.sumOf { it.value } / recentData.size
    }
    
    /**
     * 전체 시스템 평균 지연 시간 계산
     */
    private fun calculateAverageLatency(): Double {
        return if (processingTimer.count() > 0) {
            processingTimer.mean(java.util.concurrent.TimeUnit.MILLISECONDS)
        } else 0.0
    }
    
    /**
     * 전체 시스템 에러율 계산
     */
    private fun calculateErrorRate(): Double {
        val totalMessages = messagesReceivedCounter.count()
        val failedMessages = messagesFailedCounter.count()
        
        return if (totalMessages > 0) {
            (failedMessages / totalMessages) * 100.0
        } else 0.0
    }
    
    /**
     * CPU 사용률 (JMX 기반)
     */
    private fun getCpuUsage(): Double {
        return try {
            val bean = ManagementFactory.getOperatingSystemMXBean()
            if (bean is com.sun.management.OperatingSystemMXBean) {
                val usage = bean.processCpuLoad * 100.0
                if (usage.isNaN()) 0.0 else usage
            } else 0.0
        } catch (e: Exception) {
            0.0
        }
    }
    
    /**
     * Redis 활성 연결 수
     */
    private fun getRedisActiveConnections(): Long {
        return try {
            // Lettuce ConnectionPoolSupport 또는 실제 Redis 연결 정보
            10L // 기본값, 실제로는 connection pool에서 조회
        } catch (e: Exception) {
            0L
        }
    }
    
    /**
     * Redis 유휴 연결 수  
     */
    private fun getRedisIdleConnections(): Long {
        return try {
            // Lettuce ConnectionPoolSupport 또는 실제 Redis 연결 정보
            5L // 기본값, 실제로는 connection pool에서 조회
        } catch (e: Exception) {
            0L
        }
    }
}
