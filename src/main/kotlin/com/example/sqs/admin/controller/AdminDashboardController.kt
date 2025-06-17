package com.example.sqs.admin.controller

import com.example.sqs.admin.domain.ActivityLog
import com.example.sqs.admin.domain.AdminDashboard
import com.example.sqs.admin.domain.SystemAlert
import com.example.sqs.admin.service.ActivityLogService
import com.example.sqs.admin.service.AdminDashboardService
import com.example.sqs.admin.service.AlertService
import com.example.sqs.common.ApiResponse
import com.example.sqs.common.exception.EntityNotFoundException
import org.springframework.web.bind.annotation.*

@RestController
@RequestMapping("/api/v1/admin/dashboard")
class AdminDashboardController(
    private val adminDashboardService: AdminDashboardService,
    private val alertService: AlertService,
    private val activityLogService: ActivityLogService
) {
    
    /**
     * 대시보드 정보를 조회합니다.
     */
    @GetMapping
    suspend fun getDashboard(): ApiResponse<AdminDashboard> {
        val dashboard = adminDashboardService.getDashboard()
        return ApiResponse.success(dashboard)
    }
    
    /**
     * 실시간 큐 통계를 조회합니다.
     */
    @GetMapping("/queues/stats")
    suspend fun getQueueStats(): ApiResponse<Map<String, Map<String, Any>>> {
        val stats = adminDashboardService.getQueueStats()
        return ApiResponse.success(stats)
    }
    
    /**
     * 활성화된 알림 목록을 조회합니다.
     */
    @GetMapping("/alerts")
    fun getAlerts(): ApiResponse<List<SystemAlert>> {
        val alerts = alertService.getActiveAlerts()
        return ApiResponse.success(alerts)
    }
    
    /**
     * 알림 상세 정보를 조회합니다.
     */
    @GetMapping("/alerts/{alertId}")
    fun getAlert(@PathVariable alertId: String): ApiResponse<SystemAlert> {
        val alert = alertService.getAlert(alertId) 
            ?: throw EntityNotFoundException("알림을 찾을 수 없습니다", "ALERT_NOT_FOUND")
        return ApiResponse.success(alert)
    }
    
    /**
     * 알림을 해결 처리합니다.
     */
    @PostMapping("/alerts/{alertId}/resolve")
    fun resolveAlert(@PathVariable alertId: String): ApiResponse<Boolean> {
        val resolved = alertService.resolveAlert(alertId)
        return ApiResponse.success(resolved)
    }
    
    /**
     * 경고 내역을 조회합니다.
     */
    @GetMapping("/alerts/history")
    fun getAlertHistory(
        @RequestParam(defaultValue = "7") daysAgo: Int
    ): ApiResponse<List<SystemAlert>> {
        val history = alertService.getAlertHistory(daysAgo)
        return ApiResponse.success(history)
    }
    
    /**
     * 최근 활동 로그를 조회합니다.
     */
    @GetMapping("/activities")
    fun getActivities(
        @RequestParam(defaultValue = "20") limit: Int
    ): ApiResponse<List<ActivityLog>> {
        val activities = activityLogService.getRecentActivities(limit)
        return ApiResponse.success(activities)
    }
}