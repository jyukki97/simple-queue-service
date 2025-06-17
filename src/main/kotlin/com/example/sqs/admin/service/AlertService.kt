package com.example.sqs.admin.service

import com.example.sqs.admin.domain.AlertSeverity
import com.example.sqs.admin.domain.AlertType
import com.example.sqs.admin.domain.SystemAlert
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

@Service
class AlertService {
    
    /**
     * 현재 활성화된 시스템 알림 목록을 가져옵니다.
     */
    fun getActiveAlerts(): List<SystemAlert> {
        // Mock 구현 - 실제로는 DB에서 가져옵니다
        return (1..Random.nextInt(0, 3)).map { i ->
            val type = AlertType.values().random()
            val title = when (type) {
                AlertType.MEMORY_HIGH -> "시스템 메모리 사용량 높음"
                AlertType.DISK_FULL -> "디스크 공간 부족"
                AlertType.QUEUE_OVERFLOW -> "큐 오버플로우 발생"
                AlertType.DLQ_THRESHOLD -> "DLQ 임계값 초과"
                AlertType.TENANT_QUOTA_EXCEEDED -> "테넌트 할당량 초과"
            }
            
            val message = when (type) {
                AlertType.MEMORY_HIGH -> "현재 메모리 사용량이 80%를 초과하였습니다."
                AlertType.DISK_FULL -> "디스크 사용량이 90%를 초과하였습니다."
                AlertType.QUEUE_OVERFLOW -> "큐 'tenant-${i}/queue-${i}'의 메시지 수가 임계값을 초과하였습니다."
                AlertType.DLQ_THRESHOLD -> "DLQ 'tenant-${i}/dlq-${i}'의 메시지가 10개를 초과하였습니다."
                AlertType.TENANT_QUOTA_EXCEEDED -> "테넌트 'tenant-${i}'가 API 호출 할당량을 초과하였습니다."
            }
            
            val severity = when (Random.nextInt(0, 4)) {
                0 -> AlertSeverity.LOW
                1 -> AlertSeverity.MEDIUM
                2 -> AlertSeverity.HIGH
                else -> AlertSeverity.CRITICAL
            }
            
            SystemAlert(
                id = UUID.randomUUID().toString(),
                type = type,
                title = title,
                message = message,
                severity = severity,
                timestamp = Instant.now().minusSeconds(Random.nextLong(60, 3600)),
                isResolved = false
            )
        }
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
