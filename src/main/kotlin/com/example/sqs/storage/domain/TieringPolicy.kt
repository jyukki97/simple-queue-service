package com.example.sqs.storage.domain

import com.example.sqs.queue.domain.Queue
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * 공통 계층화 정책 인터페이스
 * 모든 큐 타입(FIFO, Standard)에서 사용할 수 있는 기본 계층화 기능을 정의
 */
interface TieringPolicy {
    
    companion object {
        // 공통 설정값들
        const val DEFAULT_HOT_MAX_COUNT = 100L
        const val DEFAULT_WARM_MAX_COUNT = 1000L
        const val DEFAULT_HOT_TIME_THRESHOLD_HOURS = 1L
        const val DEFAULT_WARM_TIME_THRESHOLD_HOURS = 24L
    }
    
    /**
     * Hot → Warm 이동 조건 확인
     */
    fun shouldMoveHotToWarm(stats: TierStats): Boolean
    
    /**
     * Warm → Cold 이동 조건 확인
     */
    fun shouldMoveWarmToCold(stats: TierStats): Boolean
    
    /**
     * Hot → Warm으로 이동할 메시지들 선택
     */
    fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String>
    
    /**
     * Warm → Cold로 이동할 메시지들 선택
     */
    fun getMessagesToMoveWarmToCold(queue: Queue, stats: TierStats): Flux<String>
    
    /**
     * 큐 타입별 설정값 조회
     */
    fun getHotMaxCount(): Long = DEFAULT_HOT_MAX_COUNT
    fun getWarmMaxCount(): Long = DEFAULT_WARM_MAX_COUNT
    fun getHotTimeThresholdHours(): Long = DEFAULT_HOT_TIME_THRESHOLD_HOURS
    fun getWarmTimeThresholdHours(): Long = DEFAULT_WARM_TIME_THRESHOLD_HOURS
}

/**
 * FIFO 큐 전용 계층화 정책
 * 순서 보장이 중요한 FIFO 큐의 특수한 요구사항을 처리
 */
interface FifoTieringPolicy : TieringPolicy {
    
    companion object {
        // FIFO 전용 설정값들
        const val HOT_MAX_COUNT = 100L
        const val WARM_MAX_COUNT = 1000L
        const val HOT_TIME_THRESHOLD_HOURS = 1L
        const val WARM_TIME_THRESHOLD_HOURS = 24L
    }
    
    /**
     * Hot-Warm 스왑 조건 확인 (FIFO 전용)
     * Hot이 가득 차고 Warm에 더 오래된 메시지가 있는 경우
     */
    fun shouldSwapHotWarm(hotStats: TierStats, warmStats: TierStats): Boolean
    
    /**
     * Hot-Warm 스왑 실행 (FIFO 전용)
     */
    fun swapHotWarm(queue: Queue, hotStats: TierStats, warmStats: TierStats): Mono<SwapResult>
    
    override fun getHotMaxCount(): Long = HOT_MAX_COUNT
    override fun getWarmMaxCount(): Long = WARM_MAX_COUNT
    override fun getHotTimeThresholdHours(): Long = HOT_TIME_THRESHOLD_HOURS
    override fun getWarmTimeThresholdHours(): Long = WARM_TIME_THRESHOLD_HOURS
}

/**
 * Standard 큐 전용 계층화 정책
 * 순서가 중요하지 않은 Standard 큐의 간단한 계층화 처리
 */
interface StandardTieringPolicy : TieringPolicy {
    
    companion object {
        // Standard 전용 설정값들 (좀 더 관대한 설정)
        const val HOT_MAX_COUNT = 100L
        const val WARM_MAX_COUNT = 1000L
        const val HOT_TIME_THRESHOLD_HOURS = 2L
        const val WARM_TIME_THRESHOLD_HOURS = 48L
    }
    
    override fun getHotMaxCount(): Long = HOT_MAX_COUNT
    override fun getWarmMaxCount(): Long = WARM_MAX_COUNT
    override fun getHotTimeThresholdHours(): Long = HOT_TIME_THRESHOLD_HOURS
    override fun getWarmTimeThresholdHours(): Long = WARM_TIME_THRESHOLD_HOURS
}

/**
 * 스왑 연산 결과
 */
data class SwapResult(
    val hotToWarmCount: Long,
    val warmToHotCount: Long,
    val executionTimeMs: Long
)
