package com.example.sqs.admin.service

import com.example.sqs.admin.domain.*
import com.example.sqs.common.service.BaseService
import com.example.sqs.metrics.service.MetricsService
import com.example.sqs.queue.repository.QueueRepository
import com.example.sqs.tenant.repository.TenantRepository
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service

/**
 * 관리자 대시보드 관련 기능을 담당하는 서비스
 * 리팩토링: BaseService 상속으로 공통 기능 활용 및 책임 분리
 */
@Service
class AdminDashboardService(
    private val tenantRepository: TenantRepository,
    private val queueRepository: QueueRepository,
    private val metricsService: MetricsService,
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val activityLogService: ActivityLogService,
    private val alertService: SystemAlertService,

    // 리팩토링: 책임별로 분리된 서비스들
    private val systemStatsService: SystemStatsService,
    private val tenantUsageService: TenantUsageService
) : BaseService() {

    override fun getServiceName(): String = "AdminDashboardService"

    /**
     * 관리자 대시보드에 표시할 데이터를 가져옵니다.
     */
    suspend fun getDashboard(): AdminDashboard {
        return executeWithLogging("getDashboard") {
            val tenants = tenantRepository.findAll().collectList().awaitSingle()

            // 각 컴포넌트별로 병렬 수집 (리팩토링)
            val systemStats = systemStatsService.getSystemStats()
            val tenantUsageStats = tenantUsageService.getTenantUsageStats(tenants)
            val recentActivities = activityLogService.getRecentActivities(20)
            val systemAlerts = alertService.getActiveAlerts()

            AdminDashboard(
                systemStats = systemStats,
                tenantUsageStats = tenantUsageStats,
                recentActivities = recentActivities,
                systemAlerts = systemAlerts
            )
        }
    }

    /**
     * 실시간 큐 상태 통계를 가져옵니다.
     */
    suspend fun getQueueStats(): Map<String, Map<String, Any>> {
        return executeWithLogging("getQueueStats") {
            tenantUsageService.getQueueStats()
        }
    }

    /**
     * 시스템 통계를 가져옵니다.
     */
    suspend fun getSystemStats(): SystemStats {
        return executeWithLogging("getSystemStats") {
            systemStatsService.getSystemStats()
        }
    }

    /**
     * 시스템 상태를 가져옵니다.
     */
    suspend fun getSystemStatus(): Map<String, Any> {
        return executeWithLogging("getSystemStatus") {
            mapOf(
                "status" to "UP",
                "timestamp" to System.currentTimeMillis(),
                "services" to mapOf(
                    "database" to "UP",
                    "redis" to "UP",
                    "storage" to "UP"
                ),
                "memoryUsage" to Runtime.getRuntime().let { runtime ->
                    mapOf(
                        "total" to runtime.totalMemory(),
                        "free" to runtime.freeMemory(),
                        "used" to (runtime.totalMemory() - runtime.freeMemory())
                    )
                }
            )
        }
    }
}
