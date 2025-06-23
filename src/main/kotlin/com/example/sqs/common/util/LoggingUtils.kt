package com.example.sqs.common.util

import org.slf4j.Logger
import kotlin.system.measureTimeMillis

/**
 * 로깅 관련 유틸리티 클래스
 */
object LoggingUtils {
    
    /**
     * 실행 시간을 측정하고 로깅합니다
     */
    suspend fun <T> logExecutionTime(
        operation: String,
        logger: Logger,
        block: suspend () -> T
    ): T {
        val result: T
        val timeMillis = measureTimeMillis {
            result = block()
        }
        
        // 장시간 실행되는 작업만 로깅 (1초 이상)
        if (timeMillis > 1000) {
            logger.info("Long running operation completed: $operation in ${timeMillis}ms")
        }
        return result
    }
}
