package com.example.sqs.queue.service

import com.example.sqs.performance.partitioning.QueuePartitioningService
import com.example.sqs.queue.domain.Queue
import org.slf4j.LoggerFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.io.File

@Service
class QueueCleanupService(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val partitioningService: QueuePartitioningService
) {

    private val logger = LoggerFactory.getLogger(QueueCleanupService::class.java)

    /**
     * 큐 삭제 시 모든 관련 메시지와 키들을 정리
     */
    fun cleanupQueueData(queue: Queue): Mono<QueueCleanupResult> {
        logger.info("큐 ${queue.queueName} 데이터 정리 시작 (FIFO: ${queue.attributes.fifoQueue})")

        val cleanupTasks = mutableListOf<Mono<Long>>()

        if (queue.attributes.fifoQueue == true) {
            // FIFO 큐: 파티션 없는 단일 키 구조
            cleanupTasks.addAll(cleanupFifoQueueKeys(queue))
        } else {
            // Standard 큐: 파티션별 키 구조
            cleanupTasks.addAll(cleanupStandardQueueKeys(queue))
        }

        // 공통 정리 작업
        cleanupTasks.addAll(cleanupCommonKeys(queue))

        return Flux.merge(cleanupTasks)
            .collectList()
            .flatMap { deletedCounts ->
                val totalDeleted = deletedCounts.sum()
                cleanupColdStorageFiles(queue)
                    .map { deletedFiles ->
                        QueueCleanupResult(
                            queueName = queue.queueName,
                            deletedRedisKeys = totalDeleted,
                            deletedColdFiles = deletedFiles,
                            success = true
                        )
                    }
            }
            .doOnSuccess { result ->
                logger.info("큐 ${queue.queueName} 정리 완료: Redis 키 ${result.deletedRedisKeys}개, 콜드 파일 ${result.deletedColdFiles}개 삭제")
            }
            .doOnError { error ->
                logger.error("큐 ${queue.queueName} 정리 중 오류 발생", error)
            }
            .onErrorReturn(
                QueueCleanupResult(
                    queueName = queue.queueName,
                    deletedRedisKeys = 0,
                    deletedColdFiles = 0,
                    success = false
                )
            )
    }

    /**
     * FIFO 큐 키들 정리
     */
    private fun cleanupFifoQueueKeys(queue: Queue): List<Mono<Long>> {
        val tasks = mutableListOf<Mono<Long>>()

        // FIFO 큐 메인 키들
        val fifoKeys = listOf(
            "queue:${queue.tenantId}:${queue.queueName}",
            "hot:queue:${queue.tenantId}:${queue.queueName}",
            "warm:queue:${queue.tenantId}:${queue.queueName}",
            "cold:queue:${queue.tenantId}:${queue.queueName}",
            "hot:index:${queue.tenantId}:${queue.queueName}",
            "warm:index:${queue.tenantId}:${queue.queueName}",
            "cold:index:${queue.tenantId}:${queue.queueName}"
        )

        fifoKeys.forEach { key ->
            tasks.add(deleteKeyAndCollectMessages(key))
        }

        return tasks
    }

    /**
     * Standard 큐 키들 정리 (파티션별)
     */
    private fun cleanupStandardQueueKeys(queue: Queue): List<Mono<Long>> {
        val tasks = mutableListOf<Mono<Long>>()
        val partitionCount = 4 // 설정에서 가져와야 함

        for (partitionNumber in 0 until partitionCount) {
            val partitionedQueueName = partitioningService.getPartitionedQueueName(queue.queueName, partitionNumber)

            val partitionKeys = listOf(
                "hot:queue:${queue.tenantId}:$partitionedQueueName",
                "warm:queue:${queue.tenantId}:$partitionedQueueName",
                "cold:queue:${queue.tenantId}:$partitionedQueueName",
                "hot:index:${queue.tenantId}:$partitionedQueueName",
                "warm:index:${queue.tenantId}:$partitionedQueueName",
                "cold:index:${queue.tenantId}:$partitionedQueueName"
            )

            partitionKeys.forEach { key ->
                tasks.add(deleteKeyAndCollectMessages(key))
            }
        }

        return tasks
    }

    /**
     * 공통 키들 정리
     */
    private fun cleanupCommonKeys(queue: Queue): List<Mono<Long>> {
        val tasks = mutableListOf<Mono<Long>>()

        // 추적 인덱스 삭제
        val traceKey = "trace:index:${queue.tenantId}:${queue.queueName}"
        tasks.add(redisTemplate.delete(traceKey))

        return tasks
    }

    /**
     * 키를 삭제하면서 포함된 메시지 ID들도 수집해서 개별 메시지 데이터 삭제
     */
    private fun deleteKeyAndCollectMessages(queueKey: String): Mono<Long> {
        return redisTemplate.opsForList()
            .range(queueKey, 0, -1)  // 모든 메시지 ID 수집
            .collectList()
            .flatMap { messageIds ->
                if (messageIds.isNotEmpty()) {
                    // 개별 메시지 데이터 삭제
                    val messageKeys = messageIds.map { "message:$it" }
                    val deleteMessageTask = if (messageKeys.isNotEmpty()) {
                        redisTemplate.delete(*messageKeys.toTypedArray())
                    } else {
                        Mono.just(0L)
                    }

                    // 큐 키 삭제
                    val deleteQueueKeyTask = redisTemplate.delete(queueKey)

                    Mono.zip(deleteMessageTask, deleteQueueKeyTask)
                        .map { tuple -> tuple.t1 + tuple.t2 }
                } else {
                    // 메시지가 없으면 키만 삭제
                    redisTemplate.delete(queueKey)
                }
            }
            .doOnNext { deletedCount ->
                if (deletedCount > 0) {
                    logger.debug("키 $queueKey 정리 완료: $deletedCount 개 키 삭제")
                }
            }
    }

    /**
     * 콜드 스토리지 파일들 삭제
     */
    private fun cleanupColdStorageFiles(queue: Queue): Mono<Long> {
        return Mono.fromCallable {
            val coldStoragePath = "/tmp/sqs-cold-storage"
            val queueDir = File(coldStoragePath, "${queue.tenantId}/${queue.queueName}")

            var deletedFiles = 0L
            if (queueDir.exists()) {
                queueDir.walkTopDown().forEach { file ->
                    if (file.isFile) {
                        try {
                            if (file.delete()) {
                                deletedFiles++
                                logger.debug("콜드 파일 삭제: ${file.absolutePath}")
                            }
                        } catch (e: Exception) {
                            logger.warn("콜드 파일 삭제 실패: ${file.absolutePath}", e)
                        }
                    }
                }

                // 빈 디렉토리 삭제
                try {
                    queueDir.deleteRecursively()
                } catch (e: Exception) {
                    logger.warn("콜드 디렉토리 삭제 실패: ${queueDir.absolutePath}", e)
                }
            }

            deletedFiles
        }
            .doOnNext { deletedFiles ->
                if (deletedFiles > 0) {
                    logger.info("콜드 스토리지 정리 완료: $deletedFiles 개 파일 삭제")
                }
            }
            .onErrorReturn(0L)
    }
}

/**
 * 큐 정리 결과
 */
data class QueueCleanupResult(
    val queueName: String,
    val deletedRedisKeys: Long,
    val deletedColdFiles: Long,
    val success: Boolean
)
