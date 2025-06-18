package com.example.sqs.admin.service

import com.example.sqs.admin.domain.AlertSeverity
import com.example.sqs.admin.domain.AlertType
import com.example.sqs.admin.domain.SystemAlert
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

@Service
class SystemAlertService {
    
    /**
     * 현재 활성화된 시스템 알림 목록을 가져옵니다.
     */
    fun getActiveAlerts(): List<SystemAlert> {
        // 실제 시스템 상태를 기반으로 알림을 생성합니다
        val alerts = mutableListOf<SystemAlert>()
        
        // JVM 메모리 사용량 체크
        val runtime = Runtime.getRuntime()
        val usedMemory = runtime.totalMemory() - runtime.freeMemory()
        val maxMemory = runtime.maxMemory()
        val memoryUsage = if (maxMemory > 0) usedMemory.toDouble() / maxMemory * 100 else 0.0
        
        if (memoryUsage > 80) {
            alerts.add(
                SystemAlert(
                    id = UUID.randomUUID().toString(),
                    type = AlertType.MEMORY_HIGH,
                    title = "시스템 메모리 사용량 높음",
                    message = "현재 메모리 사용량이 ${String.format("%.1f", memoryUsage)}%입니다.",
                    severity = if (memoryUsage > 90) AlertSeverity.CRITICAL else AlertSeverity.HIGH,
                    timestamp = Instant.now(),
                    isResolved = false
                )
            )
        }
        
        // 실제 시스템에 문제가 없으면 빈 리스트 반환
        return alerts
    }
    
    /**
     * 경고 ID로 시스템 경고를 조회합니다.
     */
    fun getAlert(alertId: String): SystemAlert? {
        // Mock 구현 - 실제로는 DB에서 조회합니다
        return getActiveAlerts().find { it.id == alertId }
    }
    
    /**
     * 경고를 해결 처리합니다.
     */
    fun resolveAlert(alertId: String): Boolean {
        // Mock 구현 - 실제로는 DB에서 상태를 업데이트합니다
        val alert = getAlert(alertId) ?: throw com.example.sqs.common.exception.EntityNotFoundException("알림을 찾을 수 없습니다", "ALERT_NOT_FOUND")
        println("Alert resolved: $alertId")
        return true
    }
    
    /**
     * 지정된 기간 동안의 경고 내역을 조회합니다.
     */
    fun getAlertHistory(daysAgo: Int): List<SystemAlert> {
        // Mock 구현 - 실제로는 DB에서 조회합니다
        return (1..Random.nextInt(5, 20)).map { i ->
            val type = AlertType.values().random()
            val severity = AlertSeverity.values().random()
            SystemAlert(
                id = "history-alert-$i",
                type = type,
                title = "과거 경고 #$i",
                message = "과거에 발생했던 ${type.name} 경고입니다.",
                severity = severity,
                timestamp = Instant.now().minusSeconds((daysAgo * 86400 / 2).toLong()),
                isResolved = true
            )
        }
    }
}
