package com.example.sqs.admin.service

import com.example.sqs.admin.domain.*
import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.repository.TenantRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

@Service
class AdminDashboardService(
    private val tenantRepository: TenantRepository,
    private val activityLogService: ActivityLogService,
    private val alertService: AlertService
) {

    /**
     * 관리자 대시보드에 표시할 데이터를 가져옵니다.
     */
    suspend fun getDashboard(): AdminDashboard {
        // 전체 테넌트 목록을 가져옵니다 (Mock 구현으로 변경)
        val tenants = listOf(
            Tenant(
                tenantId = "tenant-1",
                apiKey = "sqs_test1",
                name = "테스트 테넌트 1",
                createdAt = Instant.now().minusSeconds(86400 * 7),
                isActive = true
            ),
            Tenant(
                tenantId = "tenant-2",
                apiKey = "sqs_test2",
                name = "테스트 테넌트 2",
                createdAt = Instant.now().minusSeconds(86400 * 3),
                isActive = true
            )
        )
        
        // 시스템 통계를 구성합니다 (Mock 데이터)
        val systemStats = SystemStats(
            totalTenants = tenants.size.toLong(),
            totalQueues = tenants.size * Random.nextInt(2, 10).toLong(),
            totalMessages = tenants.size * Random.nextInt(100, 1000).toLong(),
            totalMessagesProcessed = tenants.size * Random.nextInt(500, 5000).toLong(),
            systemUptime = Random.nextLong(3600, 2592000), // 1시간에서 30일 사이
            memoryUsage = MemoryUsage(
                usedMemory = Random.nextLong(100_000_000, 500_000_000),
                maxMemory = 1_000_000_000,
                usagePercentage = Random.nextDouble(30.0, 80.0)
            ),
            redisStats = RedisStats(
                connectedClients = Random.nextInt(5, 20),
                usedMemory = Random.nextLong(50_000_000, 200_000_000),
                keyCount = Random.nextLong(1000, 10000),
                opsPerSecond = Random.nextDouble(100.0, 500.0)
            )
        )
        
        // 테넌트별 사용량 통계
        val tenantUsageStats = tenants.map { tenant ->
            TenantUsageStats(
                tenantId = tenant.tenantId,
                tenantName = tenant.name,
                queueCount = Random.nextLong(1, 10),
                messageCount = Random.nextLong(50, 5000),
                messagesProcessedToday = Random.nextLong(10, 1000),
                storageUsed = Random.nextLong(1024, 1024 * 100),
                apiCallsToday = Random.nextLong(10, 1000),
                lastActivity = Instant.now().minusSeconds(Random.nextLong(60, 86400)),
                isActive = tenant.isActive
            )
        }
        
        // 최근 활동 로그
        val recentActivities = activityLogService.getRecentActivities(20)
        
        // 시스템 알림
        val systemAlerts = alertService.getActiveAlerts()
        
        return AdminDashboard(
            systemStats = systemStats,
            tenantUsageStats = tenantUsageStats,
            recentActivities = recentActivities,
            systemAlerts = systemAlerts
        )
    }
    
    /**
     * 실시간 큐 상태 통계를 가져옵니다.
     */
    suspend fun getQueueStats(): Map<String, Map<String, Any>> {
        // 임의의 큐 상태 데이터 생성 (Mock 데이터)
        val tenants = listOf(
            Tenant(
                tenantId = "tenant-1",
                apiKey = "sqs_test1",
                name = "테스트 테넌트 1",
                createdAt = Instant.now(),
                isActive = true
            ),
            Tenant(
                tenantId = "tenant-2",
                apiKey = "sqs_test2",
                name = "테스트 테넌트 2",
                createdAt = Instant.now(),
                isActive = true
            )
        )
        
        return tenants.associate { tenant ->
            val queueCount = Random.nextInt(1, 5)
            val queueStats = (1..queueCount).associate { queueIndex ->
                val queueName = "queue-$queueIndex"
                queueName to mapOf<String, Any>(
                    "messagesAvailable" to Random.nextInt(0, 100),
                    "messagesInFlight" to Random.nextInt(0, 50),
                    "messagesSentPerMinute" to Random.nextInt(1, 60),
                    "messagesReceivedPerMinute" to Random.nextInt(1, 55),
                    "oldestMessageAge" to Random.nextInt(0, 3600),
                    "dlqMessageCount" to Random.nextInt(0, 10)
                )
            }
            tenant.tenantId to queueStats
        }
    }
}