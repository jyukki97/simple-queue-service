package com.example.sqs.storage.scheduler

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.repository.QueueRepository
import com.example.sqs.storage.domain.FifoTieringPolicy
import com.example.sqs.storage.domain.StandardTieringPolicy
import com.example.sqs.storage.domain.TierStats
import com.example.sqs.storage.domain.TieringPolicy
import com.example.sqs.storage.dto.BatchMoveResult
import com.example.sqs.storage.service.StorageStatsService
import com.example.sqs.storage.service.WarmTierManagerService
import org.slf4j.LoggerFactory
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers

@Component
@ConditionalOnProperty(value = ["app.storage.tiering.enabled"], havingValue = "true", matchIfMissing = true)
class TieringScheduler(
    private val queueRepository: QueueRepository,
    private val fifoTieringPolicy: FifoTieringPolicy,
    private val standardTieringPolicy: StandardTieringPolicy,
    private val storageStatsService: StorageStatsService,
    private val warmTierManagerService: WarmTierManagerService
) {

    private val logger = LoggerFactory.getLogger(TieringScheduler::class.java)

    /**
     * 5분마다 계층화 정책 실행 - 모든 큐 타입 지원
     */
    @Scheduled(fixedDelayString = "\${app.storage.tiering.check-interval-seconds:300}000")
    fun executeTieringPolicy() {
        logger.info("계층화 정책 실행 시작 - 모든 큐 타입 지원")

        try {
            // 모든 큐를 조회하여 타입별로 계층화 정책 적용
            queueRepository.findAllQueues()
                .flatMap { queue ->
                    if (queue.attributes.fifoQueue) {
                        processFifoQueueTiering(queue)
                    } else {
                        processStandardQueueTiering(queue)
                    }
                }
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    { },
                    { error -> logger.error("계층화 정책 실행 중 오류 발생", error) },
                    { logger.info("모든 큐 계층화 정책 실행 완료") }
                )
        } catch (e: Exception) {
            logger.error("계층화 정책 실행 중 예외 발생", e)
        }
    }

    /**
     * FIFO 큐 계층화 처리
     */
    private fun processFifoQueueTiering(queue: Queue): Mono<String> {
        return Mono.zip(
            storageStatsService.getHotStats(queue),
            storageStatsService.getWarmStats(queue),
            storageStatsService.getColdStats(queue)
        ).flatMap { tuple ->
            val hotStats = tuple.t1
            val warmStats = tuple.t2
            val coldStats = tuple.t3

            // FIFO 전용: Hot-Warm 스왑 처리
            if (fifoTieringPolicy.shouldSwapHotWarm(hotStats, warmStats)) {
                logger.info("FIFO Hot-Warm 스왑 실행: ${queue.queueName}")
                fifoTieringPolicy.swapHotWarm(queue, hotStats, warmStats)
                    .doOnNext { result ->
                        logger.info("FIFO 스왑 완료: ${queue.queueName}, hotToWarm=${result.hotToWarmCount}, warmToHot=${result.warmToHotCount}")
                        // TODO: 메트릭 기록 추가
                    }
                    .map { "FIFO 스왑 완료: ${queue.queueName}" }
            } else {
                processTieringMoves(queue, hotStats, warmStats, coldStats, fifoTieringPolicy, "FIFO")
            }
        }
    }

    /**
     * Standard 큐 계층화 처리
     */
    private fun processStandardQueueTiering(queue: Queue): Mono<String> {
        return Mono.zip(
            storageStatsService.getHotStats(queue),
            storageStatsService.getWarmStats(queue),
            storageStatsService.getColdStats(queue)
        ).flatMap { tuple ->
            val hotStats = tuple.t1
            val warmStats = tuple.t2
            val coldStats = tuple.t3
            processTieringMoves(queue, hotStats, warmStats, coldStats, standardTieringPolicy, "Standard")
        }
    }

    /**
     * 공통 티어링 이동 처리
     */
    private fun processTieringMoves(
        queue: Queue,
        hotStats: TierStats,
        warmStats: TierStats,
        coldStats: TierStats,
        policy: TieringPolicy,
        queueType: String
    ): Mono<String> {
        val operations = mutableListOf<Mono<String>>()

        // Hot → Warm 이동 (새로운 RocksDB WARM 계층 사용)
        if (policy.shouldMoveHotToWarm(hotStats)) {
            logger.info("$queueType Hot→Warm 이동 시작: ${queue.queueName}")
            val moveOperation = policy.getMessagesToMoveHotToWarm(queue, hotStats)
                .collectList()
                .flatMap { messageIds ->
                    if (messageIds.isNotEmpty()) {
                        logger.info("$queueType Hot→Warm 이동 대상: ${messageIds.size}개 메시지")
                        // 새로운 RocksDB WARM 계층으로 배치 이동
                        warmTierManagerService.moveMessagesToWarmBatch(messageIds, queue.tenantId, queue.queueName)
                            .defaultIfEmpty(BatchMoveResult(0, 0, 0, listOf("결과가 null입니다")))
                            .doOnSuccess { result ->
                                if (result != null) {
                                    logger.info("$queueType Hot→Warm 배치 이동 완료: ${queue.queueName}")
                                    logger.info("  - 성공: ${result.successCount}개")
                                    logger.info("  - 실패: ${result.failCount}개")
                                    if (result.errors.isNotEmpty()) {
                                        logger.warn("  - 오류: ${result.errors}")
                                    }
                                } else {
                                    logger.warn("$queueType Hot→Warm 배치 이동 결과가 null입니다: ${queue.queueName}")
                                }
                            }
                            .map { result -> "$queueType Hot→Warm: ${result?.successCount ?: 0}/${result?.totalRequested ?: 0} 메시지 이동 완료" }
                    } else {
                        Mono.just("$queueType Hot→Warm: 이동할 메시지 없음")
                    }
                }

            operations.add(moveOperation)
        }

        // Warm → Cold 이동 (배치 처리로 개선)
        if (policy.shouldMoveWarmToCold(warmStats)) {
            logger.info("$queueType Warm→Cold 이동 시작: ${queue.queueName}")
            val moveOperation = policy.getMessagesToMoveWarmToCold(queue, warmStats)
                .collectList()
                .flatMap { messageIds ->
                    if (messageIds.isNotEmpty()) {
                        logger.info("$queueType Warm→Cold 배치 이동: ${queue.queueName}, ${messageIds.size}개 메시지")
                        storageStatsService.moveMessagesBatchWarmToCold(messageIds, queue)
                    } else {
                        Mono.just(0L)
                    }
                }
                .doOnNext { count ->
                    logger.info("$queueType Warm→Cold 배치 이동 완료: ${queue.queueName}, 이동된 메시지: ${count}개")
                    // TODO: 메트릭 기록 추가
                }
                .map { count -> "$queueType Warm→Cold: ${count}개 메시지 배치 이동" }

            operations.add(moveOperation)
        }

        return if (operations.isNotEmpty()) {
            Mono.zip(operations) { results ->
                "$queueType 계층화 완료: ${queue.queueName} - ${results.joinToString(", ")}"
            }
        } else {
            Mono.just("$queueType 계층화 조건 미충족: ${queue.queueName}")
        }
    }

    /**
     * 1시간마다 WARM 계층 만료된 메시지 정리
     */
    @Scheduled(fixedDelayString = "\${app.storage.warm.cleanup-interval-seconds:3600}000")
    fun cleanupWarmExpiredMessages() {
        logger.info("WARM 계층 만료 메시지 정리 시작")

        try {
            warmTierManagerService.cleanupExpiredMessages()
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    { count ->
                        logger.info("WARM 계층 만료 메시지 정리 완료: ${count}개")
                    },
                    { error ->
                        logger.error("WARM 계층 만료 메시지 정리 중 오류 발생", error)
                    }
                )
        } catch (e: Exception) {
            logger.error("WARM 계층 정리 스케줄러 실행 중 예외 발생", e)
        }
    }
}
