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
        // Mock implementation
        return (1..limit).map { i ->
            ActivityLog(
                id = "activity-$i",
                tenantId = "tenant-${(1..5).random()}",
                action = listOf("QUEUE_CREATED", "MESSAGE_SENT", "TENANT_UPDATED", "API_KEY_RESET").random(),
                description = "Mock activity detail $i",
                timestamp = Instant.now().minusSeconds((i * 60).toLong()),
                severity = ActivitySeverity.INFO
            )
        }
    }
}
