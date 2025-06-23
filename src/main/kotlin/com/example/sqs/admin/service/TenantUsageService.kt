package com.example.sqs.admin.service

import com.example.sqs.admin.domain.TenantUsageStats
import com.example.sqs.common.service.BaseService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.repository.QueueRepository
import com.example.sqs.tenant.domain.Tenant
import kotlinx.coroutines.reactor.awaitSingle
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import java.time.Instant
import kotlin.random.Random

/**
 * 테넌트 사용량 통계 관련 기능을 담당하는 서비스
 */
@Service
class TenantUsageService(
    private val queueRepository: QueueRepository,
    private val redisTemplate: ReactiveRedisTemplate<String, String>
) : BaseService() {

    override fun getServiceName(): String = "TenantUsageService"

    /**
     * 테넌트별 사용량 통계를 가져옵니다
     */
    suspend fun getTenantUsageStats(tenants: List<Tenant>): List<TenantUsageStats> {
        return executeWithLogging("getTenantUsageStats") {
            tenants.map { tenant ->
                calculateTenantUsage(tenant)
            }
        }
    }

    /**
     * 실시간 큐 상태 통계를 가져옵니다
     */
    suspend fun getQueueStats(): Map<String, Map<String, Any>> {
        return executeWithLogging("getQueueStats") {
            // TODO: 실제 큐 데이터로 대체
            generateMockQueueStats()
        }
    }

    /**
     * 개별 테넌트의 사용량을 계산합니다
     */
    private suspend fun calculateTenantUsage(tenant: Tenant): TenantUsageStats {
        val queues = queueRepository.findByTenantId(tenant.tenantId, 1000, 0, null)
            .collectList()
            .awaitSingle<List<Queue>>()

        val queueCount = queues.size
        var messageCount = 0L

        // 각 큐의 메시지 수 집계
        for (queue in queues) {
            messageCount += calculateQueueMessageCount(tenant.tenantId, queue.queueName)
        }

        return TenantUsageStats(
            tenantId = tenant.tenantId,
            tenantName = tenant.name,
            queueCount = queueCount.toLong(),
            messageCount = messageCount,
            messagesProcessedToday = calculateDailyProcessedMessages(tenant.tenantId),
            storageUsed = calculateStorageUsage(tenant.tenantId),
            apiCallsToday = calculateDailyApiCalls(tenant.tenantId),
            lastActivity = getLastActivity(tenant.tenantId),
            isActive = tenant.isActive
        )
    }

    /**
     * 큐의 메시지 수를 계산합니다
     */
    private suspend fun calculateQueueMessageCount(tenantId: String, queueName: String): Long {
        val availableKey = "queue:messages:$tenantId:$queueName"
        val inflightKey = "queue:inflight:$tenantId:$queueName"
        val delayedKey = "queue:delayed:$tenantId:$queueName"

        return try {
            val available = redisTemplate.opsForList().size(availableKey).awaitSingle()
            val inflight = redisTemplate.opsForZSet().count(inflightKey, Range.unbounded()).awaitSingle()
            val delayed = redisTemplate.opsForZSet().count(delayedKey, Range.unbounded()).awaitSingle()

            available + inflight + delayed
        } catch (e: Exception) {
            logger.warn("[${getServiceName()}] Failed to calculate message count for queue: $tenantId:$queueName", e)
            0L
        }
    }

    /**
     * 일일 처리된 메시지 수 계산 (더미 값)
     */
    private fun calculateDailyProcessedMessages(@Suppress("UNUSED_PARAMETER") tenantId: String): Long {
        // TODO: 실제 메트릭 데이터에서 가져오기
        return Random.nextLong(1000, 50000)
    }

    /**
     * 스토리지 사용량 계산 (더미 값)
     */
    private fun calculateStorageUsage(@Suppress("UNUSED_PARAMETER") tenantId: String): Long {
        // TODO: 실제 스토리지 사용량 계산
        return Random.nextLong(1024 * 1024, 1024 * 1024 * 100) // 1MB ~ 100MB
    }

    /**
     * 일일 API 호출 수 계산 (더미 값)
     */
    private fun calculateDailyApiCalls(@Suppress("UNUSED_PARAMETER") tenantId: String): Long {
        // TODO: 실제 API 호출 통계에서 가져오기
        return Random.nextLong(100, 10000)
    }

    /**
     * 마지막 활동 시간 조회 (더미 값)
     */
    private fun getLastActivity(@Suppress("UNUSED_PARAMETER") tenantId: String): Instant {
        // TODO: 실제 활동 로그에서 가져오기
        return Instant.now().minusSeconds(Random.nextLong(300, 3600)) // 5분 ~ 1시간 전
    }

    /**
     * 모의 큐 통계 생성 (개발용)
     */
    private fun generateMockQueueStats(): Map<String, Map<String, Any>> {
        val tenantCount = Random.nextInt(2, 6)
        return (1..tenantCount).associate { tenantIndex ->
            val tenantId = "tenant-$tenantIndex"
            val queueCount = Random.nextInt(1, 5)
            val queueStats = (1..queueCount).associate { queueIndex ->
                val queueName = "queue-$queueIndex"
                queueName to mapOf<String, Any>(
                    "messagesAvailable" to Random.nextInt(0, 100),
                    "messagesInFlight" to Random.nextInt(0, 50),
                    "messagesSentPerMinute" to Random.nextInt(1, 60),
                    "messagesReceivedPerMinute" to Random.nextInt(1, 55),
                    "approximateAgeOfOldestMessage" to Random.nextInt(0, 3600),
                    "messagesDelayed" to Random.nextInt(0, 20)
                )
            }
            tenantId to queueStats
        }
    }
}
