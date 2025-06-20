package com.example.sqs.storage.domain

import com.example.sqs.queue.domain.Queue
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

/**
 * FIFO Queue 전용 계층화 정책
 *
 * 이제 TieringPolicy.kt에서 정의된 FifoTieringPolicy 인터페이스를 사용합니다.
 * 이 파일은 하위 호환성을 위해 유지됩니다.
 *
 * 핵심 개념:
 * - 개수 기반 이관: Hot 100개, Warm 1000개 제한
 * - 최신 메시지 우선 이관: 순서 보장을 위해 오래된 메시지를 Hot에 유지
 * - 스왑 메커니즘: Hot-Warm 간 메시지 교환으로 순서 최적화
 */
interface FifoTieringPolicy : TieringPolicy {

    companion object {
        const val HOT_MAX_COUNT = 100
        const val WARM_MAX_COUNT = 1000
        const val HOT_TIME_THRESHOLD_HOURS = 1L
        const val WARM_TIME_THRESHOLD_HOURS = 24L
    }

    /**
     * Hot → Warm 이관이 필요한지 확인
     */
    override fun shouldMoveHotToWarm(stats: TierStats): Boolean

    /**
     * Warm → Cold 이관이 필요한지 확인
     */
    override fun shouldMoveWarmToCold(stats: TierStats): Boolean

    /**
     * Hot-Warm 스왑이 필요한지 확인
     */
    fun shouldSwapHotWarm(hotStats: TierStats, warmStats: TierStats): Boolean

    /**
     * Hot → Warm으로 이관할 메시지 ID 목록 반환
     * - 개수 초과: 최신 메시지들 이관
     * - 시간 초과: 오래된 메시지들 이관
     */
    override fun getMessagesToMoveHotToWarm(queue: Queue, stats: TierStats): Flux<String>

    /**
     * Warm → Cold로 이관할 메시지 ID 목록 반환
     */
    override fun getMessagesToMoveWarmToCold(queue: Queue, stats: TierStats): Flux<String>

    /**
     * Hot-Warm 스왑 수행
     */
    fun swapHotWarm(queue: Queue, hotStats: TierStats, warmStats: TierStats): Mono<SwapResult>
}

/**
 * 스왑 연산 결과
 */
data class SwapResult(
    val hotToWarmCount: Long,
    val warmToHotCount: Long,
    val executionTimeMs: Long
)
