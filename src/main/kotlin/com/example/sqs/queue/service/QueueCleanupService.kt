package com.example.sqs.queue.service

import com.example.sqs.common.redis.RedisKeyManager
import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueCleanupResult
import com.example.sqs.storage.service.WarmTierManagerService
import com.example.sqs.storage.service.impl.ColdStorageService
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Lazy
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * 큐 정리 관련 기능을 담당하는 서비스
 */
@Service
class QueueCleanupService(
    @Lazy private val warmTierManagerService: WarmTierManagerService,
    @Lazy private val coldStorageService: ColdStorageService,
    private val redisKeyManager: RedisKeyManager,
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    @Value("\${app.queue.partition-count:4}") private val partitionCount: Int
) : BaseReactiveService() {

    override fun getServiceName(): String = "QueueCleanupService"

    /**
     * 큐 삭제 시 모든 관련 메시지와 키들을 정리
     */
    fun cleanupQueueData(queue: Queue): Mono<QueueCleanupResult> {
        logger.info("[${getServiceName()}] Starting queue data cleanup: ${queue.queueName} (FIFO: ${queue.attributes.fifoQueue})")

        return Mono.fromCallable {
            QueueCleanupResult(
                queueName = queue.queueName,
                deletedRedisKeys = 0,
                deletedColdFiles = 0,
                success = true
            )
        }
            .flatMap { result ->
                // 1. HOT 계층 메시지 삭제 (Redis) - 모든 큐 타입에서 파티션 키 삭제
                deleteAllQueueKeys(queue)
                    .then(
                        // 2. WARM 계층 메시지 삭제 (RocksDB)
                        warmTierManagerService.deleteAllQueueMessages(queue.tenantId, queue.queueName)
                            .doOnSuccess { deletedCount ->
                                logger.info("WARM 계층에서 ${deletedCount}개 메시지 삭제")
                            }
                            .onErrorResume { error ->
                                logger.warn("WARM 계층 메시지 삭제 실패: ${queue.queueName}", error)
                                Mono.just(0)
                            }
                            .then()
                    )
                    .then(
                        // 3. COLD 계층 메시지 삭제 (파일 시스템)
                        Mono.fromCallable {
                            kotlinx.coroutines.runBlocking {
                                coldStorageService.deleteQueueMessages(queue.tenantId, queue.queueName)
                            }
                        }
                            .doOnSuccess { success ->
                                logger.info("COLD 계층 메시지 삭제 결과: $success")
                            }
                            .onErrorResume { error ->
                                logger.warn("COLD 계층 메시지 삭제 실패: ${queue.queueName}", error)
                                Mono.just(false)
                            }
                            .then()
                    )
                    .then(Mono.just(result.copy(success = true)))
            }
            .doOnSuccess { result ->
                logger.info(
                    "[${getServiceName()}] Queue cleanup completed: ${queue.queueName}, " +
                            "Redis keys: ${result.deletedRedisKeys}, Cold files: ${result.deletedColdFiles}"
                )
            }
            .onErrorReturn(
                QueueCleanupResult(
                    queueName = queue.queueName,
                    deletedRedisKeys = 0,
                    deletedColdFiles = 0,
                    success = false,
                    error = "Cleanup failed"
                )
            )
    }

    /**
     * 모든 큐의 파티션별 키들을 삭제 (FIFO/Standard 공통)
     */
    private fun deleteAllQueueKeys(queue: Queue): Mono<Void> {
        logger.info("큐 파티션별 키 삭제 시작: ${queue.queueName}")

        // 파티션별 HOT 키들 생성
        val hotKeys = (0 until partitionCount).map { partition ->
            redisKeyManager.getAvailableQueueKey(queue, partition)
        }

        // 기본 키들 (delayed, inflight, 파티션 없는 hot 키도 포함)
        val basicKeys = listOf(
            redisKeyManager.getAvailableQueueKey(queue.tenantId, queue.queueName), // 파티션 없는 키도 혹시 몰라서
            redisKeyManager.getDelayedQueueKey(queue.tenantId, queue.queueName),
            redisKeyManager.getProcessingQueueKey(queue.tenantId, queue.queueName)
        )

        val allKeys = hotKeys + basicKeys

        return redisTemplate.delete(*allKeys.toTypedArray())
            .doOnSuccess { deletedCount ->
                logger.info("큐 키 삭제 완료: ${queue.queueName}, 삭제된 키 수: $deletedCount")
            }
            .then()
    }
}
