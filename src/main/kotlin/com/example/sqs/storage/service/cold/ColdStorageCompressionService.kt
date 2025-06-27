package com.example.sqs.storage.service.cold

import com.example.sqs.storage.domain.cold.SystemMetrics
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.stereotype.Component

/**
 * 콜드 스토리지 압축 관리 서비스
 */
@Component
class ColdStorageCompressionService(
    @Value("\${app.storage.cold.phase2.dynamic-compression-enabled:true}")
    private val dynamicCompressionEnabled: Boolean = true
) {
    private val logger = LoggerFactory.getLogger(ColdStorageCompressionService::class.java)

    /**
     * 시스템 상태에 따른 동적 압축 레벨 결정
     */
    fun determineCompressionLevel(): Int {
        if (!dynamicCompressionEnabled) {
            return 6 // 기본값
        }

        val metrics = SystemMetrics.getCurrent()

        return when {
            metrics.cpuUsage > 0.8 -> {
                logger.debug("CPU 사용률 높음(${metrics.cpuUsage}), 빠른 압축 사용")
                1 // CPU 높으면 빠른 압축
            }
            metrics.cpuUsage < 0.3 -> {
                logger.debug("CPU 사용률 낮음(${metrics.cpuUsage}), 최대 압축 사용")
                9 // CPU 여유로우면 최대 압축
            }
            else -> {
                logger.debug("CPU 사용률 보통(${metrics.cpuUsage}), 기본 압축 사용")
                6 // 기본값
            }
        }
    }

    /**
     * 압축 설정 정보 조회
     */
    fun getCompressionInfo(): Map<String, Any> {
        return mapOf(
            "dynamic_compression_enabled" to dynamicCompressionEnabled,
            "current_compression_level" to determineCompressionLevel(),
            "current_cpu_usage" to SystemMetrics.getCurrent().cpuUsage
        )
    }
}
