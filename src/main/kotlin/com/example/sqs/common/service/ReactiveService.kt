package com.example.sqs.common.service

import reactor.core.publisher.Mono
import reactor.core.publisher.Flux

/**
 * 리액티브 서비스를 위한 공통 인터페이스
 * 표준화된 비동기 처리와 에러 핸들링을 제공합니다.
 */
interface ReactiveService {
    
    /**
     * 서비스 이름을 반환합니다
     */
    fun getServiceName(): String
    
    /**
     * 서비스 상태를 확인합니다
     */
    fun healthCheck(): Mono<Boolean> = Mono.just(true)
}

/**
 * 리액티브 서비스의 기본 구현체
 */
abstract class BaseReactiveService : ReactiveService {
    
    protected val logger = org.slf4j.LoggerFactory.getLogger(this::class.java)
    
    /**
     * 공통 에러 처리를 위한 메서드
     */
    protected fun <T> handleError(operation: String): (Throwable) -> Mono<T> = { throwable ->
        logger.error("[${getServiceName()}] $operation failed", throwable)
        Mono.error(throwable)
    }
    
    /**
     * 여러 결과를 처리할 때의 에러 핸들링
     */
    protected fun <T> handleFluxError(operation: String): (Throwable) -> Flux<T> = { throwable ->
        logger.error("[${getServiceName()}] $operation failed", throwable)
        Flux.error(throwable)
    }
}
