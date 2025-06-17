package com.example.sqs.scheduler.service

import com.example.sqs.message.repository.MessageRepository
import com.example.sqs.queue.repository.QueueRepository
import org.slf4j.LoggerFactory
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Instant

/**
 * 큐 관련 정기 작업을 수행하는 스케줄러 서비스
 */
@Service
class ScheduledTaskService(
    private val queueRepository: QueueRepository,
    private val messageRepository: MessageRepository
) {

    private val logger = LoggerFactory.getLogger(ScheduledTaskService::class.java)

    /**
     * 가시성 타임아웃이 만료된 메시지들을 다시 available 상태로 복원
     * 30초마다 실행
     */
    @Scheduled(fixedRate = 30000) // 30초
    fun restoreExpiredVisibilityTimeouts() {
        logger.info("가시성 타임아웃 복원 작업 시작")
        
        queueRepository.findAllQueues()
            .flatMap { queue ->
                messageRepository.restoreExpiredMessages(queue)
                    .doOnSuccess { restoredCount ->
                        if (restoredCount > 0) {
                            logger.info("가시성 타임아웃 복원 완료: queue=${queue.queueName}, restored=$restoredCount")
                        }
                    }
                    .onErrorContinue { error, _ ->
                        logger.error("가시성 타임아웃 복원 중 오류 발생: queue=${queue.queueName}", error)
                    }
            }
            .then()
            .doOnSuccess { logger.debug("모든 큐의 가시성 타임아웃 복원 작업 완료") }
            .doOnError { error -> logger.error("가시성 타임아웃 복원 작업 중 전체 오류 발생", error) }
            .subscribe()
    }

    /**
     * 지연된 메시지들 중 실행 시간이 된 것들을 available 상태로 이동
     * 10초마다 실행
     */
    @Scheduled(fixedRate = 10000) // 10초
    fun processDelayedMessages() {
        logger.debug("지연 메시지 처리 작업 시작")
        
        queueRepository.findAllQueues()
            .flatMap { queue ->
                messageRepository.moveDelayedToAvailable(queue)
                    .doOnSuccess { movedCount ->
                        if (movedCount > 0) {
                            logger.info("지연 메시지 처리 완료: queue=${queue.queueName}, moved=$movedCount")
                        }
                    }
                    .onErrorContinue { error, _ ->
                        logger.error("지연 메시지 처리 중 오류 발생: queue=${queue.queueName}", error)
                    }
            }
            .then()
            .doOnSuccess { logger.debug("모든 큐의 지연 메시지 처리 작업 완료") }
            .doOnError { error -> logger.error("지연 메시지 처리 작업 중 전체 오류 발생", error) }
            .subscribe()
    }

    /**
     * 만료된 메시지들을 정리 (보존 기간 초과)
     * 1시간마다 실행
     */
    @Scheduled(fixedRate = 3600000) // 1시간
    fun cleanupExpiredMessages() {
        logger.info("만료 메시지 정리 작업 시작")
        
        queueRepository.findAllQueues()
            .flatMap { queue ->
                val retentionPeriod = queue.attributes.messageRetentionPeriod
                val cutoffTime = Instant.now().minusSeconds(retentionPeriod.toLong())
                
                cleanupExpiredMessagesForQueue(queue.tenantId, queue.queueName, cutoffTime)
                    .doOnSuccess { cleanedCount ->
                        if (cleanedCount > 0) {
                            logger.info("만료 메시지 정리 완료: queue=${queue.queueName}, cleaned=$cleanedCount")
                        }
                    }
                    .onErrorContinue { error, _ ->
                        logger.error("만료 메시지 정리 중 오류 발생: queue=${queue.queueName}", error)
                    }
            }
            .then()
            .doOnSuccess { logger.info("모든 큐의 만료 메시지 정리 작업 완료") }
            .doOnError { error -> logger.error("만료 메시지 정리 작업 중 전체 오류 발생", error) }
            .subscribe()
    }

    /**
     * DLQ 이동 처리 (최대 수신 횟수 초과 메시지)
     * 1분마다 실행
     */
    @Scheduled(fixedRate = 60000) // 1분
    fun processDLQTransfer() {
        logger.debug("DLQ 이동 처리 작업 시작")
        
        queueRepository.findAllQueues()
            .filter { queue -> queue.attributes.deadLetterTargetArn != null }
            .flatMap { queue ->
                processDLQTransferForQueue(queue.tenantId, queue.queueName, queue.attributes.maxReceiveCount)
                    .doOnSuccess { transferredCount ->
                        if (transferredCount > 0) {
                            logger.info("DLQ 이동 처리 완료: queue=${queue.queueName}, transferred=$transferredCount")
                        }
                    }
                    .onErrorContinue { error, _ ->
                        logger.error("DLQ 이동 처리 중 오류 발생: queue=${queue.queueName}", error)
                    }
            }
            .then()
            .doOnSuccess { logger.debug("모든 큐의 DLQ 이동 처리 작업 완료") }
            .doOnError { error -> logger.error("DLQ 이동 처리 작업 중 전체 오류 발생", error) }
            .subscribe()
    }

    /**
     * 특정 큐의 만료된 메시지들을 정리합니다
     */
    private fun cleanupExpiredMessagesForQueue(tenantId: String, queueName: String, cutoffTime: Instant): Mono<Long> {
        // TODO: Redis에서 만료된 메시지들을 찾아서 삭제하는 로직 구현
        // 현재는 기본 구현으로 0을 반환
        return Mono.just(0L)
    }

    /**
     * 특정 큐에서 최대 수신 횟수를 초과한 메시지들을 DLQ로 이동합니다
     */
    private fun processDLQTransferForQueue(tenantId: String, queueName: String, maxReceiveCount: Int): Mono<Long> {
        // TODO: Redis에서 수신 횟수가 maxReceiveCount를 초과한 메시지들을 찾아서 DLQ로 이동하는 로직 구현
        // 현재는 기본 구현으로 0을 반환
        return Mono.just(0L)
    }
}
