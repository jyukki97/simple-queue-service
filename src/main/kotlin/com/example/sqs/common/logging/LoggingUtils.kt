package com.example.sqs.common.logging

import org.slf4j.Logger
import org.slf4j.LoggerFactory
import org.slf4j.MDC

/**
 * 로깅 유틸리티 클래스
 */
object LoggingUtils {
    
    /**
     * 클래스에 대한 로거 생성
     */
    inline fun <reified T> logger(): Logger = LoggerFactory.getLogger(T::class.java)
    
    /**
     * MDC에 컨텍스트 정보 설정
     */
    fun setMDC(tenantId: String? = null, queueName: String? = null, messageId: String? = null) {
        tenantId?.let { MDC.put("tenantId", it) }
        queueName?.let { MDC.put("queueName", it) }
        messageId?.let { MDC.put("messageId", it) }
    }
    
    /**
     * MDC 정리
     */
    fun clearMDC() {
        MDC.clear()
    }
    
    /**
     * 실행 시간 측정과 함께 로깅
     */
    inline fun <T> logExecutionTime(
        logger: Logger,
        operation: String,
        block: () -> T
    ): T {
        val startTime = System.currentTimeMillis()
        try {
            val result = block()
            val executionTime = System.currentTimeMillis() - startTime
            logger.info("Operation '{}' completed in {} ms", operation, executionTime)
            return result
        } catch (e: Exception) {
            val executionTime = System.currentTimeMillis() - startTime
            logger.error("Operation '{}' failed after {} ms", operation, executionTime, e)
            throw e
        }
    }
}

/**
 * 로깅 확장 함수들
 */
fun Logger.debugWithMDC(message: String, vararg args: Any?) {
    if (this.isDebugEnabled) {
        this.debug(message, *args)
    }
}

fun Logger.infoWithMDC(message: String, vararg args: Any?) {
    this.info(message, *args)
}

fun Logger.warnWithMDC(message: String, vararg args: Any?) {
    this.warn(message, *args)
}

fun Logger.errorWithMDC(message: String, throwable: Throwable? = null, vararg args: Any?) {
    if (throwable != null) {
        this.error(message, *args, throwable)
    } else {
        this.error(message, *args)
    }
}
