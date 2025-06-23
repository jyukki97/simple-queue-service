package com.example.sqs.queue.service

import com.example.sqs.common.service.BaseReactiveService
import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueCleanupResult
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

/**
 * 큐 정리 관련 기능을 담당하는 서비스
 */
@Service
class QueueCleanupService : BaseReactiveService() {

    override fun getServiceName(): String = "QueueCleanupService"

    /**
     * 큐 삭제 시 모든 관련 메시지와 키들을 정리
     */
    fun cleanupQueueData(queue: Queue): Mono<QueueCleanupResult> {
        logger.info("[${getServiceName()}] Starting queue data cleanup: ${queue.queueName} (FIFO: ${queue.attributes.fifoQueue})")

        return Mono.fromCallable {
            // TODO: 실제 cleanup 로직 구현
            QueueCleanupResult(
                queueName = queue.queueName,
                deletedRedisKeys = 0,
                deletedColdFiles = 0,
                success = true
            )
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
}
