package com.example.sqs.metrics.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.metrics.domain.*
import com.example.sqs.metrics.service.AlertService
import com.example.sqs.metrics.service.MetricsService
import com.example.sqs.tenant.service.TenantService
import org.springframework.http.MediaType
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant
import java.time.temporal.ChronoUnit

/**
 * 메트릭 및 모니터링 API 컨트롤러
 */
@RestController
@RequestMapping("/api/v1/metrics")
class MetricsController(
    private val metricsService: MetricsService,
    private val alertService: AlertService,
    private val tenantService: TenantService
) {
    
    /**
     * 큐별 메트릭 조회
     */
    @GetMapping("/queues/{queueName}")
    fun getQueueMetrics(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Mono<ApiResponse<QueueMetrics>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMap { tenant ->
                metricsService.collectQueueMetrics(tenant.tenantId, queueName)
            }
            .map { metrics ->
                ApiResponse.success(metrics)
            }
    }
    
    /**
     * 테넌트의 모든 큐 메트릭 조회
     */
    @GetMapping("/queues")
    fun getAllQueueMetrics(
        @RequestHeader("X-API-Key") apiKey: String
    ): Mono<ApiResponse<List<QueueMetrics>>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMapMany { tenant ->
                metricsService.getAllQueueMetrics(tenant.tenantId)
            }
            .collectList()
            .map { metricsList ->
                ApiResponse.success(metricsList)
            }
    }
    
    /**
     * 시스템 전체 메트릭 조회
     */
    @GetMapping("/system")
    fun getSystemMetrics(
        @RequestHeader("X-API-Key") apiKey: String
    ): Mono<ApiResponse<SystemMetrics>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMap {
                metricsService.collectSystemMetrics()
            }
            .map { metrics ->
                ApiResponse.success(metrics)
            }
    }
    
    /**
     * 시계열 메트릭 조회
     */
    @GetMapping("/timeseries/{metricName}")
    fun getTimeSeriesMetrics(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable metricName: String,
        @RequestParam(defaultValue = "1") hoursBack: Long,
        @RequestParam(required = false) queueName: String?,
        @RequestParam(required = false) labels: Map<String, String>?
    ): Mono<ApiResponse<MetricTimeSeries>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMap { tenant ->
                val endTime = Instant.now()
                val startTime = endTime.minus(hoursBack, ChronoUnit.HOURS)
                
                val metricLabels = mutableMapOf<String, String>("tenant_id" to tenant.tenantId)
                queueName?.let { metricLabels["queue_name"] = it }
                labels?.let { metricLabels.putAll(it) }
                
                metricsService.getTimeSeriesMetrics(metricName, metricLabels, startTime, endTime)
            }
            .map { timeSeries ->
                ApiResponse.success(timeSeries)
            }
    }
    
    /**
     * 실시간 메트릭 스트림 (WebFlux SSE)
     */
    @GetMapping(value = ["/stream/queues/{queueName}"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamQueueMetrics(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable queueName: String
    ): Flux<QueueMetrics> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMapMany { tenant ->
                Flux.interval(java.time.Duration.ofSeconds(5))
                    .flatMap {
                        metricsService.collectQueueMetrics(tenant.tenantId, queueName)
                    }
            }
    }
    
    /**
     * 알림 규칙 생성
     */
    @PostMapping("/alerts/rules")
    fun createAlertRule(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestBody request: CreateAlertRuleRequest
    ): Mono<ApiResponse<AlertRule>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMap {
                alertService.createAlertRule(
                    name = request.name,
                    description = request.description,
                    metricName = request.metricName,
                    condition = request.condition,
                    threshold = request.threshold,
                    duration = request.duration
                )
            }
            .map { rule ->
                ApiResponse.success(rule)
            }
    }
    
    /**
     * 알림 규칙 목록 조회
     */
    @GetMapping("/alerts/rules")
    fun getAlertRules(
        @RequestHeader("X-API-Key") apiKey: String
    ): Mono<ApiResponse<List<AlertRule>>> {
        return tenantService.validateApiKey(apiKey)
            .flatMapMany {
                alertService.getAllAlertRules()
            }
            .collectList()
            .map { rules ->
                ApiResponse.success(rules)
            }
    }
    
    /**
     * 알림 규칙 조회
     */
    @GetMapping("/alerts/rules/{ruleId}")
    fun getAlertRule(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable ruleId: String
    ): Mono<ApiResponse<AlertRule?>> {
        return tenantService.validateApiKey(apiKey)
            .flatMap {
                alertService.getAlertRule(ruleId)
            }
            .map { rule ->
                ApiResponse.success(rule)
            }
    }
    
    /**
     * 알림 규칙 수정
     */
    @PutMapping("/alerts/rules/{ruleId}")
    fun updateAlertRule(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable ruleId: String,
        @RequestBody request: UpdateAlertRuleRequest
    ): Mono<ApiResponse<AlertRule?>> {
        return tenantService.validateApiKey(apiKey)
            .flatMap {
                val updatedRule = AlertRule(
                    id = ruleId,
                    name = request.name,
                    description = request.description,
                    metricName = request.metricName,
                    condition = request.condition,
                    threshold = request.threshold,
                    duration = request.duration,
                    isEnabled = request.isEnabled,
                    createdAt = Instant.now() // 실제로는 기존 값 유지
                )
                alertService.updateAlertRule(ruleId, updatedRule)
            }
            .map { rule ->
                ApiResponse.success(rule)
            }
    }
    
    /**
     * 알림 규칙 삭제
     */
    @DeleteMapping("/alerts/rules/{ruleId}")
    fun deleteAlertRule(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable ruleId: String
    ): Mono<ApiResponse<Boolean>> {
        return tenantService.validateApiKey(apiKey)
            .flatMap {
                alertService.deleteAlertRule(ruleId)
            }
            .map { deleted ->
                ApiResponse.success(deleted)
            }
    }
    
    /**
     * 알림 규칙 활성화/비활성화
     */
    @PatchMapping("/alerts/rules/{ruleId}/toggle")
    fun toggleAlertRule(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable ruleId: String,
        @RequestBody request: ToggleAlertRuleRequest
    ): Mono<ApiResponse<AlertRule?>> {
        return tenantService.validateApiKey(apiKey)
            .flatMap {
                alertService.toggleAlertRule(ruleId, request.enabled)
            }
            .map { rule ->
                ApiResponse.success(rule)
            }
    }
    
    /**
     * 실시간 알림 이벤트 스트림
     */
    @GetMapping(value = ["/alerts/events/stream"], produces = [MediaType.TEXT_EVENT_STREAM_VALUE])
    fun streamAlertEvents(
        @RequestHeader("X-API-Key") apiKey: String
    ): Flux<AlertEvent> {
        return tenantService.validateApiKey(apiKey)
            .flatMapMany {
                alertService.subscribeToAlerts()
            }
    }
    
    /**
     * 수동 알림 트리거
     */
    @PostMapping("/alerts/trigger")
    fun triggerAlert(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestBody request: TriggerAlertRequest
    ): Mono<ApiResponse<AlertEvent>> {
        return tenantService.validateApiKeyAndGetTenant(apiKey)
            .flatMap { tenant ->
                alertService.triggerAlert(
                    ruleId = request.ruleId,
                    message = request.message,
                    severity = request.severity,
                    metricValue = request.metricValue,
                    queueName = request.queueName,
                    tenantId = tenant.tenantId
                )
            }
            .map { event ->
                ApiResponse.success(event)
            }
    }
}

// Request DTOs
data class CreateAlertRuleRequest(
    val name: String,
    val description: String,
    val metricName: String,
    val condition: AlertCondition,
    val threshold: Double,
    val duration: Long
)

data class UpdateAlertRuleRequest(
    val name: String,
    val description: String,
    val metricName: String,
    val condition: AlertCondition,
    val threshold: Double,
    val duration: Long,
    val isEnabled: Boolean
)

data class ToggleAlertRuleRequest(
    val enabled: Boolean
)

data class TriggerAlertRequest(
    val ruleId: String,
    val message: String,
    val severity: AlertSeverity,
    val metricValue: Double,
    val queueName: String? = null
)
