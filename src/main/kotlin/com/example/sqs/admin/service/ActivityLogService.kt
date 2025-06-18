package com.example.sqs.admin.service

import com.example.sqs.admin.domain.ActivityLog
import com.example.sqs.admin.domain.ActivitySeverity
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono
import java.time.Instant
import java.util.UUID

@Service
class ActivityLogService {
    
    fun logActivity(
        tenantId: String,
        action: String,
        description: String,
        severity: ActivitySeverity
    ): Mono<Void> {
        // Mock implementation - in real version, this would save to database
        val activityLog = ActivityLog(
            id = UUID.randomUUID().toString(),
            tenantId = tenantId,
            action = action,
            description = description,
            timestamp = Instant.now(),
            severity = severity
        )
        
        // For now, just log to console
        println("Activity logged: $activityLog")
        
        return Mono.empty()
    }
    
    fun getRecentActivities(limit: Int = 50): List<ActivityLog> {
        // 실제 구현에서는 데이터베이스에서 활동 로그를 가져옵니다
        // 현재는 실제 활동이 없으므로 빈 리스트를 반환합니다
        return emptyList()
    }
}
