package com.example.sqs.storage.controller

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.service.QueueService
import com.example.sqs.storage.dto.*
import com.example.sqs.storage.scheduler.TieringScheduler
import com.example.sqs.storage.service.StorageStatsService
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/storage")
class StorageController(
    private val storageStatsService: StorageStatsService,
    private val queueService: QueueService,
    private val tieringScheduler: TieringScheduler
) {

    /**
     * 전체 스토리지 통계 조회
     */
    @GetMapping("/stats")
    fun getStorageStats(): Mono<StorageStatsResponse> {
        val tenantId = AuthContext.getCurrentTenantId()

        return queueService.listQueues(tenantId, limit = 100, offset = 0, namePattern = null)
            .flatMap { queue: Queue ->
                Mono.zip(
                    storageStatsService.getHotStats(queue),
                    storageStatsService.getWarmStats(queue),
                    storageStatsService.getColdStats(queue)
                ).map { tuple ->
                    QueueStorageStats(
                        queueName = queue.queueName,
                        queueType = if (queue.attributes.fifoQueue) "FIFO" else "Standard",
                        hotStats = tuple.t1,
                        warmStats = tuple.t2,
                        coldStats = tuple.t3
                    )
                }
            }
            .collectList()
            .map { queueStats: List<QueueStorageStats> ->
                StorageStatsResponse(
                    totalQueues = queueStats.size,
                    queueStats = queueStats,
                    aggregatedStats = AggregatedStats(
                        totalMessages = queueStats.sumOf { stats ->
                            stats.hotStats.messageCount + stats.warmStats.messageCount + stats.coldStats.messageCount
                        },
                        totalHotMessages = queueStats.sumOf { stats -> stats.hotStats.messageCount },
                        totalWarmMessages = queueStats.sumOf { stats -> stats.warmStats.messageCount },
                        totalColdMessages = queueStats.sumOf { stats -> stats.coldStats.messageCount },
                        totalStorageBytes = queueStats.sumOf { stats ->
                            stats.hotStats.totalSizeBytes + stats.warmStats.totalSizeBytes + stats.coldStats.totalSizeBytes
                        }
                    )
                )
            }
    }

    /**
     * 특정 큐의 스토리지 통계 조회
     */
    @GetMapping("/queues/{queueName}/stats")
    fun getQueueStorageStats(
        @PathVariable queueName: String
    ): Mono<QueueStorageStats> {
        val tenantId = AuthContext.getCurrentTenantId()

        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                Mono.zip(
                    storageStatsService.getHotStats(queue),
                    storageStatsService.getWarmStats(queue),
                    storageStatsService.getColdStats(queue)
                ).map { tuple ->
                    QueueStorageStats(
                        queueName = queue.queueName,
                        queueType = if (queue.attributes.fifoQueue) "FIFO" else "Standard",
                        hotStats = tuple.t1,
                        warmStats = tuple.t2,
                        coldStats = tuple.t3
                    )
                }
            }
    }

    /**
     * 메시지 위치 확인
     */
    @GetMapping("/messages/{messageId}/location")
    fun getMessageLocation(
        @PathVariable messageId: String
    ): Mono<MessageLocationResponse> {
        // 메시지의 현재 위치 정보 조회 (Redis에서)
        return storageStatsService.getMessageLocation(messageId)
            .map { location ->
                MessageLocationResponse(
                    messageId = messageId,
                    currentTier = location.tier.name,
                    lastMovedAt = location.lastMovedAt.toString(),
                    createdAt = location.createdAt.toString()
                )
            }
    }

    /**
     * 특정 스토리지 계층의 메시지 목록 조회
     */
    @GetMapping("/queues/{queueName}/tiers/{tier}/messages")
    fun getMessagesByTier(
        @PathVariable queueName: String,
        @PathVariable tier: String,
        @RequestParam(defaultValue = "10") maxMessages: Int,
        @RequestParam(defaultValue = "0") offset: Int
    ): Mono<TierMessagesResponse> {
        val tenantId = AuthContext.getCurrentTenantId()

        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                storageStatsService.getMessagesByTier(queue, tier, maxMessages, offset)
                    .map { messages ->
                        TierMessagesResponse(
                            queueName = queueName,
                            tier = tier,
                            messages = messages,
                            totalCount = messages.size,
                            offset = offset,
                            maxMessages = maxMessages
                        )
                    }
            }
    }

    /**
     * 전체 스토리지 계층별 메시지 요약 조회
     */
    @GetMapping("/queues/{queueName}/messages/by-tier")
    fun getMessagesSummaryByTier(
        @PathVariable queueName: String
    ): Mono<StorageMessagesSummaryResponse> {
        val tenantId = AuthContext.getCurrentTenantId()

        return queueService.findQueue(tenantId, queueName)
            .flatMap { queue ->
                Mono.zip(
                    storageStatsService.getHotStats(queue),
                    storageStatsService.getStatsByActualStorage(queue, "WARM"),
                    storageStatsService.getStatsByActualStorage(queue, "COLD"),
                    storageStatsService.getMessagesByTier(queue, "HOT", 5, 0),
                    storageStatsService.getMessagesByTier(queue, "WARM", 5, 0),
                    storageStatsService.getMessagesByTier(queue, "COLD", 5, 0)
                ).map { tuple ->
                    StorageMessagesSummaryResponse(
                        queueName = queueName,
                        hotMessages = TierMessagesSummary(
                            tier = "HOT",
                            messageCount = tuple.t1.messageCount,
                            sampleMessages = tuple.t4,
                            totalSizeBytes = tuple.t1.totalSizeBytes
                        ),
                        warmMessages = TierMessagesSummary(
                            tier = "WARM",
                            messageCount = tuple.t2.messageCount,
                            sampleMessages = tuple.t5,
                            totalSizeBytes = tuple.t2.totalSizeBytes
                        ),
                        coldMessages = TierMessagesSummary(
                            tier = "COLD",
                            messageCount = tuple.t3.messageCount,
                            sampleMessages = tuple.t6,
                            totalSizeBytes = tuple.t3.totalSizeBytes
                        )
                    )
                }
            }
    }

    /**
     * 계층화 정책 수동 실행 (테스트/디버깅용)
     */
    @PostMapping("/trigger-tiering")
    fun triggerTieringPolicy(): Mono<Map<String, Any>> {
        return try {
            tieringScheduler.executeTieringPolicy()
            Mono.just(mapOf("success" to true, "message" to "계층화 정책이 실행되었습니다."))
        } catch (e: Exception) {
            Mono.just(mapOf("success" to false, "error" to "계층화 정책 실행 실패: ${e.message}"))
        }
    }
}
