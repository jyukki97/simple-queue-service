package com.example.sqs.storage.controller

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.queue.service.QueueService
import com.example.sqs.storage.dto.*
import com.example.sqs.storage.service.StorageStatsService
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono

@RestController
@RequestMapping("/api/v1/storage")
class StorageController(
    private val storageStatsService: StorageStatsService,
    private val queueService: QueueService
) {

    /**
     * 전체 스토리지 통계 조회
     */
    @GetMapping("/stats")
    fun getStorageStats(): Mono<StorageStatsResponse> {
        val tenantId = AuthContext.getCurrentTenantId()

        return queueService.listQueues(tenantId, limit = 100, offset = 0, namePattern = null)
            .flatMap { queue ->
                Mono.zip(
                    storageStatsService.getHotStats(queue),
                    storageStatsService.getWarmStats(queue),
                    storageStatsService.getColdStats(queue)
                ).map { tuple ->
                    QueueStorageStats(
                        queueName = queue.queueName,
                        queueType = if (queue.attributes.fifoQueue == true) "FIFO" else "Standard",
                        hotStats = tuple.t1,
                        warmStats = tuple.t2,
                        coldStats = tuple.t3
                    )
                }
            }
            .collectList()
            .map { queueStats ->
                StorageStatsResponse(
                    totalQueues = queueStats.size,
                    queueStats = queueStats,
                    aggregatedStats = AggregatedStats(
                        totalMessages = queueStats.sumOf {
                            it.hotStats.messageCount + it.warmStats.messageCount + it.coldStats.messageCount
                        },
                        totalHotMessages = queueStats.sumOf { it.hotStats.messageCount },
                        totalWarmMessages = queueStats.sumOf { it.warmStats.messageCount },
                        totalColdMessages = queueStats.sumOf { it.coldStats.messageCount },
                        totalStorageBytes = queueStats.sumOf {
                            it.hotStats.totalSizeBytes + it.warmStats.totalSizeBytes + it.coldStats.totalSizeBytes
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
                        queueType = if (queue.attributes.fifoQueue == true) "FIFO" else "Standard",
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
     * 수동 메시지 승격
     */
    @PostMapping("/messages/{messageId}/promote")
    fun promoteMessage(
        @PathVariable messageId: String,
        @RequestBody request: PromoteMessageRequest
    ): Mono<PromoteMessageResponse> {
        val tenantId = AuthContext.getCurrentTenantId()

        return storageStatsService.promoteMessage(messageId, request.targetTier, tenantId)
            .map { result ->
                PromoteMessageResponse(
                    messageId = messageId,
                    fromTier = result.fromTier,
                    toTier = result.toTier,
                    success = result.success,
                    reason = request.reason
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
                    storageStatsService.getWarmStats(queue),
                    storageStatsService.getColdStats(queue),
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
}
