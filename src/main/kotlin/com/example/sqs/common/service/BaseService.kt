package com.example.sqs.common.service

import com.example.sqs.common.util.LoggingUtils
import org.slf4j.Logger
import org.slf4j.LoggerFactory

/**
 * 모든 서비스 클래스의 기본 인터페이스
 * 공통 기능과 표준화된 에러 처리를 제공합니다.
 */
abstract class BaseService {
    
    protected val logger: Logger = LoggerFactory.getLogger(this::class.java)
    
    /**
     * 서비스 작업 실행 시 공통 로깅 및 메트릭 처리
     */
    protected suspend fun <T> executeWithLogging(
        operation: String,
        block: suspend () -> T
    ): T {
        return LoggingUtils.logExecutionTime(operation, logger) {
            block()
        }
    }
    
    /**
     * 서비스 검증 로직을 위한 공통 메서드
     */
    protected fun validateRequired(value: Any?, fieldName: String) {
        require(value != null) { "$fieldName is required" }
        if (value is String) {
            require(value.isNotBlank()) { "$fieldName cannot be blank" }
        }
    }
    
    /**
     * 서비스 이름을 반환합니다 (로깅 및 메트릭용)
     */
    abstract fun getServiceName(): String
}
