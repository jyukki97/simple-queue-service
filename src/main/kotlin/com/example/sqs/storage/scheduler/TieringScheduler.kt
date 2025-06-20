package com.example.sqs.storage.scheduler

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.repository.QueueRepository
import com.example.sqs.storage.domain.FifoTieringPolicy
import com.example.sqs.storage.domain.StandardTieringPolicy
import com.example.sqs.storage.domain.TierStats
import com.example.sqs.storage.domain.TieringPolicy
import com.example.sqs.storage.service.StorageStatsService
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
    private val storageStatsService: StorageStatsService
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
                        logger.debug("FIFO 큐 처리: ${queue.queueName}")
                        processFifoQueueTiering(queue)
                    } else {
                        logger.debug("Standard 큐 처리: ${queue.queueName}")
                        processStandardQueueTiering(queue)
                    }
                }
                .subscribeOn(Schedulers.boundedElastic())
                .subscribe(
                    { result -> logger.debug("큐 계층화 완료: $result") },
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

        // Hot → Warm 이동
        if (policy.shouldMoveHotToWarm(hotStats)) {
            logger.info("$queueType Hot→Warm 이동 시작: ${queue.queueName}")
            val moveOperation = policy.getMessagesToMoveHotToWarm(queue, hotStats)
                .flatMap { messageId ->
                    storageStatsService.moveMessage(messageId, "HOT", "WARM", queue)
                        .doOnSuccess {
                            logger.debug("$queueType 메시지 이동 완료: $messageId (HOT→WARM)")
                        }
                }
                .count()
                .doOnNext { count ->
                    logger.info("$queueType Hot→Warm 이동 완료: ${queue.queueName}, 이동된 메시지: $count")
                    // TODO: 메트릭 기록 추가
                }
                .map { count -> "$queueType Hot→Warm: $count 메시지 이동" }

            operations.add(moveOperation)
        }

        // Warm → Cold 이동
        if (policy.shouldMoveWarmToCold(warmStats)) {
            logger.info("$queueType Warm→Cold 이동 시작: ${queue.queueName}")
            val moveOperation = policy.getMessagesToMoveWarmToCold(queue, warmStats)
                .flatMap { messageId ->
                    storageStatsService.moveMessage(messageId, "WARM", "COLD", queue)
                        .doOnSuccess {
                            logger.debug("$queueType 메시지 이동 완료: $messageId (WARM→COLD)")
                        }
                }
                .count()
                .doOnNext { count ->
                    logger.info("$queueType Warm→Cold 이동 완료: ${queue.queueName}, 이동된 메시지: $count")
                    // TODO: 메트릭 기록 추가
                }
                .map { count -> "$queueType Warm→Cold: $count 메시지 이동" }

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
}
