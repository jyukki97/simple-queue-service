package com.example.sqs.metrics.service

import com.example.sqs.metrics.domain.*
import org.springframework.stereotype.Service
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.publisher.Sinks
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap
import java.util.UUID

/**
 * 알림 관리 서비스
 */
@Service
class AlertService(
    private val metricsService: MetricsService
) {
    
    // 알림 규칙 저장소
    private val alertRules = ConcurrentHashMap<String, AlertRule>()
    
    // 알림 이벤트 스트림
    private val alertEventSink = Sinks.many().multicast().onBackpressureBuffer<AlertEvent>()
    private val alertEventFlux = alertEventSink.asFlux()
    
    // 활성 알림 상태 (알림 피로도 방지)
    private val activeAlerts = ConcurrentHashMap<String, Instant>()
    
    init {
        // 기본 알림 규칙 설정
        setupDefaultAlertRules()
        
        // 주기적 알림 검사 시작
        startAlertMonitoring()
    }
    
    /**
     * 알림 규칙 생성
     */
    fun createAlertRule(
        name: String,
        description: String,
        metricName: String,
        condition: AlertCondition,
        threshold: Double,
        duration: Long
    ): Mono<AlertRule> {
        return Mono.fromCallable {
            val rule = AlertRule(
                id = UUID.randomUUID().toString(),
                name = name,
                description = description,
                metricName = metricName,
                condition = condition,
                threshold = threshold,
                duration = duration,
                isEnabled = true,
                createdAt = Instant.now()
            )
            
            alertRules[rule.id] = rule
            rule
        }
    }
    
    /**
     * 알림 규칙 조회
     */
    fun getAlertRule(ruleId: String): Mono<AlertRule?> {
        return Mono.fromCallable { alertRules[ruleId] }
    }
    
    /**
     * 모든 알림 규칙 조회
     */
    fun getAllAlertRules(): Flux<AlertRule> {
        return Flux.fromIterable(alertRules.values)
    }
    
    /**
     * 알림 규칙 수정
     */
    fun updateAlertRule(ruleId: String, updatedRule: AlertRule): Mono<AlertRule?> {
        return Mono.fromCallable {
            if (alertRules.containsKey(ruleId)) {
                alertRules[ruleId] = updatedRule.copy(id = ruleId)
                alertRules[ruleId]
            } else {
                null
            }
        }
    }
    
    /**
     * 알림 규칙 삭제
     */
    fun deleteAlertRule(ruleId: String): Mono<Boolean> {
        return Mono.fromCallable {
            alertRules.remove(ruleId) != null
        }
    }
    
    /**
     * 알림 규칙 활성화/비활성화
     */
    fun toggleAlertRule(ruleId: String, enabled: Boolean): Mono<AlertRule?> {
        return Mono.fromCallable {
            alertRules[ruleId]?.let { rule ->
                val updatedRule = rule.copy(isEnabled = enabled)
                alertRules[ruleId] = updatedRule
                updatedRule
            }
        }
    }
    
    /**
     * 알림 이벤트 스트림 구독
     */
    fun subscribeToAlerts(): Flux<AlertEvent> {
        return alertEventFlux
    }
    
    /**
     * 수동 알림 트리거
     */
    fun triggerAlert(
        ruleId: String,
        message: String,
        severity: AlertSeverity,
        metricValue: Double,
        queueName: String? = null,
        tenantId: String? = null
    ): Mono<AlertEvent> {
        return getAlertRule(ruleId)
            .mapNotNull { rule ->
                if (rule != null) {
                    val alertEvent = AlertEvent(
                        id = UUID.randomUUID().toString(),
                        ruleId = ruleId,
                        ruleName = rule.name,
                        message = message,
                        severity = severity,
                        timestamp = Instant.now(),
                        metricValue = metricValue,
                        threshold = rule.threshold,
                        queueName = queueName,
                        tenantId = tenantId
                    )
                    
                    publishAlert(alertEvent)
                    alertEvent
                } else {
                    null
                }
            }
    }
    
    private fun setupDefaultAlertRules() {
        // 큐 크기 임계값 알림
        alertRules["queue-size-high"] = AlertRule(
            id = "queue-size-high",
            name = "큐 크기 높음",
            description = "큐에 대기 중인 메시지가 1000개를 초과합니다",
            metricName = "messages_available",
            condition = AlertCondition.GREATER_THAN,
            threshold = 1000.0,
            duration = 300, // 5분
            isEnabled = true,
            createdAt = Instant.now()
        )
        
        // DLQ 메시지 누적 알림
        alertRules["dlq-messages-high"] = AlertRule(
            id = "dlq-messages-high",
            name = "DLQ 메시지 누적",
            description = "DLQ에 메시지가 누적되고 있습니다",
            metricName = "messages_in_dlq",
            condition = AlertCondition.GREATER_THAN,
            threshold = 10.0,
            duration = 60, // 1분
            isEnabled = true,
            createdAt = Instant.now()
        )
        
        // 높은 에러율 알림
        alertRules["error-rate-high"] = AlertRule(
            id = "error-rate-high",
            name = "높은 에러율",
            description = "에러율이 5%를 초과합니다",
            metricName = "error_rate",
            condition = AlertCondition.GREATER_THAN,
            threshold = 5.0,
            duration = 120, // 2분
            isEnabled = true,
            createdAt = Instant.now()
        )
        
        // 처리 지연 알림
        alertRules["processing-delay-high"] = AlertRule(
            id = "processing-delay-high",
            name = "처리 지연 높음",
            description = "평균 처리 시간이 5초를 초과합니다",
            metricName = "average_processing_time",
            condition = AlertCondition.GREATER_THAN,
            threshold = 5000.0,
            duration = 180, // 3분
            isEnabled = true,
            createdAt = Instant.now()
        )
    }
    
    private fun startAlertMonitoring() {
        // 실제 환경에서는 스케줄러를 사용하여 주기적으로 실행
        // 여기서는 시연 목적으로 간단히 구현
    }
    
    private fun checkAlertConditions() {
        alertRules.values.filter { it.isEnabled }.forEach { rule ->
            checkRuleCondition(rule)
        }
    }
    
    private fun checkRuleCondition(rule: AlertRule) {
        // 실제 환경에서는 메트릭 값을 조회하여 임계값과 비교
        // 여기서는 시연 목적으로 랜덤 값 사용
        val currentValue = when (rule.metricName) {
            "messages_available" -> kotlin.random.Random.nextDouble(800.0, 1200.0)
            "messages_in_dlq" -> kotlin.random.Random.nextDouble(0.0, 20.0)
            "error_rate" -> kotlin.random.Random.nextDouble(0.0, 8.0)
            "average_processing_time" -> kotlin.random.Random.nextDouble(3000.0, 7000.0)
            else -> 0.0
        }
        
        val conditionMet = when (rule.condition) {
            AlertCondition.GREATER_THAN -> currentValue > rule.threshold
            AlertCondition.LESS_THAN -> currentValue < rule.threshold
            AlertCondition.EQUAL_TO -> currentValue == rule.threshold
            AlertCondition.NOT_EQUAL_TO -> currentValue != rule.threshold
        }
        
        if (conditionMet) {
            val alertKey = "${rule.id}:${rule.metricName}"
            val lastAlert = activeAlerts[alertKey]
            val now = Instant.now()
            
            // 알림 피로도 방지: 같은 규칙으로 5분 이내 중복 알림 방지
            if (lastAlert == null || now.minusSeconds(300).isAfter(lastAlert)) {
                val severity = when {
                    rule.metricName == "error_rate" && currentValue > 7.0 -> AlertSeverity.CRITICAL
                    rule.metricName == "messages_in_dlq" && currentValue > 15.0 -> AlertSeverity.ERROR
                    currentValue > rule.threshold * 1.5 -> AlertSeverity.ERROR
                    else -> AlertSeverity.WARNING
                }
                
                val alertEvent = AlertEvent(
                    id = UUID.randomUUID().toString(),
                    ruleId = rule.id,
                    ruleName = rule.name,
                    message = "${rule.description} (현재 값: $currentValue, 임계값: ${rule.threshold})",
                    severity = severity,
                    timestamp = now,
                    metricValue = currentValue,
                    threshold = rule.threshold
                )
                
                publishAlert(alertEvent)
                activeAlerts[alertKey] = now
            }
        }
    }
    
    private fun publishAlert(alertEvent: AlertEvent) {
        alertEventSink.tryEmitNext(alertEvent)
    }
}
