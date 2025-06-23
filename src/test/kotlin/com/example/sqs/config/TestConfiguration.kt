package com.example.sqs.config

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import com.example.sqs.metrics.service.MetricsService
import com.example.sqs.queue.repository.QueueRepository
import org.springframework.data.redis.core.ReactiveRedisTemplate
import io.micrometer.core.instrument.MeterRegistry
import org.mockito.Mockito
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.EventListener
import com.example.sqs.tenant.service.TenantService
import org.springframework.beans.factory.annotation.Autowired
import kotlinx.coroutines.runBlocking

/**
 * 테스트 환경용 설정
 * RocksDB 및 메트릭 관련 Bean을 Mock으로 대체
 */
@Configuration
@Profile("test")
class TestConfiguration {

    @Autowired
    private lateinit var tenantService: TenantService

    /**
     * 테스트 환경에서 MetricsService를 Mock으로 대체
     * Prometheus 메트릭 중복 등록 문제 해결
     */
    @Bean
    @Primary
    @ConditionalOnProperty(name = ["app.metrics.enable-prometheus"], havingValue = "false", matchIfMissing = true)
    fun mockMetricsService(): MetricsService {
        val mock = Mockito.mock(MetricsService::class.java)
        
        // Mock 메서드들이 null이 아닌 기본값을 반환하도록 설정
        Mockito.`when`(mock.getAllQueueMetrics(Mockito.anyString()))
            .thenReturn(reactor.core.publisher.Flux.empty())
        
        Mockito.`when`(mock.collectQueueMetrics(Mockito.anyString(), Mockito.anyString()))
            .thenReturn(reactor.core.publisher.Mono.just(
                com.example.sqs.metrics.domain.QueueMetrics(
                    queueName = "test-queue",
                    tenantId = "test-tenant",
                    timestamp = java.time.Instant.now(),
                    messagesAvailable = 0,
                    messagesInFlight = 0,
                    messagesDelayed = 0,
                    messagesInDlq = 0,
                    messagesSentPerMinute = 0.0,
                    messagesReceivedPerMinute = 0.0,
                    messagesDeletedPerMinute = 0.0,
                    messagesFailedPerMinute = 0.0,
                    averageAgeOfOldestMessage = 0,
                    averageProcessingTime = 0.0,
                    errorRate = 0.0,
                    totalMessageSize = 0,
                    averageMessageSize = 0.0
                )
            ))
        
        Mockito.`when`(mock.collectSystemMetrics())
            .thenReturn(reactor.core.publisher.Mono.just(
                com.example.sqs.metrics.domain.SystemMetrics(
                    timestamp = java.time.Instant.now(),
                    totalQueues = 0,
                    totalMessages = 0,
                    totalTenants = 0,
                    totalThroughputPerSecond = 0.0,
                    averageLatency = 0.0,
                    errorRate = 0.0,
                    memoryUsageBytes = 0,
                    cpuUsagePercent = 0.0,
                    redisConnectionsActive = 0,
                    redisConnectionsIdle = 0
                )
            ))
        
        return mock
    }

    /**
     * 테스트 환경에서 기본 테넌트 데이터 초기화
     */
    @EventListener(ApplicationReadyEvent::class)
    fun initializeTestData() {
        runBlocking {
            try {
                // 기본 테스트 테넌트들 생성
                val testTenants = listOf(
                    Triple("test-tenant", "test-api-key", "Test Tenant"),
                    Triple("test-tenant-metrics", "test-api-key-metrics", "Metrics Test Tenant")
                )

                testTenants.forEach { (tenantId, apiKey, name) ->
                    try {
                        tenantService.createTenant(tenantId, apiKey, name).subscribe()
                    } catch (e: Exception) {
                        // 이미 존재하는 테넌트는 무시
                    }
                }
            } catch (e: Exception) {
                // 테스트 데이터 초기화 실패 시 무시 (일부 테스트에서는 필요하지 않을 수 있음)
            }
        }
    }
}
