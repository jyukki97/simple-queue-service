package com.example.sqs.storage.domain.cold

/**
 * Phase 2 시스템 메트릭 수집
 */
data class SystemMetrics(
    val cpuUsage: Double,
    val memoryUsage: Double,
    val ioWait: Double,
    val diskUsage: Double
) {
    companion object {
        fun getCurrent(): SystemMetrics {
            // 실제 환경에서는 시스템 메트릭을 수집
            // 데모를 위해 시뮬레이션된 값 사용
            return SystemMetrics(
                cpuUsage = Math.random() * 0.8,
                memoryUsage = Math.random() * 0.6,
                ioWait = Math.random() * 0.3,
                diskUsage = Math.random() * 0.5
            )
        }
    }
}
