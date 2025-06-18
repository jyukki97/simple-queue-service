package com.example.sqs.tracing.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.exception.*
import com.example.sqs.tracing.domain.MessageTrace
import com.example.sqs.tracing.domain.TraceStatistics
import com.example.sqs.tracing.service.MessageTraceService
import com.example.sqs.tenant.service.TenantService
import org.slf4j.LoggerFactory
import org.springframework.format.annotation.DateTimeFormat
import org.springframework.web.bind.annotation.*
import reactor.core.publisher.Mono
import java.time.Instant
import java.time.LocalDateTime
import java.time.ZoneOffset

/**
 * 메시지 추적 컨트롤러
 */
@RestController
@RequestMapping("/api/v1/tracing")
class MessageTraceController(
    private val messageTraceService: MessageTraceService,
    private val tenantService: TenantService
) {
    private val logger = LoggerFactory.getLogger(MessageTraceController::class.java)

    /**
     * 특정 메시지의 추적 정보를 조회합니다
     */
    @GetMapping("/messages/{messageId}")
    fun getMessageTrace(
        @RequestHeader("X-API-Key") apiKey: String,
        @PathVariable messageId: String
    ): Mono<ApiResponse<MessageTrace>> {
        logger.info("메시지 추적 정보 조회 요청: messageId=$messageId")
        
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageTraceService.getMessageTrace(messageId)
                    .map { trace ->
                        // 테넌트 권한 확인
                        if (trace.tenantId != tenantId) {
                            throw AuthorizationException("접근 권한이 없습니다")
                        }
                        logger.info("메시지 추적 정보 조회 완료: messageId=$messageId, events=${trace.events.size}")
                        ApiResponse.success(trace)
                    }
                    .switchIfEmpty(Mono.error(EntityNotFoundException("메시지 추적 정보를 찾을 수 없습니다")))
            }
    }

    /**
     * 특정 기간의 메시지 추적 목록을 조회합니다
     */
    @GetMapping("/messages")
    fun getMessageTraces(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestParam(required = false) queueName: String?,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startTime: LocalDateTime,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endTime: LocalDateTime,
        @RequestParam(defaultValue = "100") limit: Int
    ): Mono<ApiResponse<List<MessageTrace>>> {
        logger.info("메시지 추적 목록 조회 요청: queueName=$queueName, period=$startTime~$endTime")
        
        val startInstant = startTime.toInstant(ZoneOffset.UTC)
        val endInstant = endTime.toInstant(ZoneOffset.UTC)
        
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageTraceService.getMessageTraces(tenantId, queueName, startInstant, endInstant, limit)
                    .collectList()
                    .map { traces ->
                        logger.info("메시지 추적 목록 조회 완료: count=${traces.size}")
                        ApiResponse.success(traces)
                    }
            }
    }

    /**
     * 추적 통계를 조회합니다
     */
    @GetMapping("/statistics")
    fun getTraceStatistics(
        @RequestHeader("X-API-Key") apiKey: String,
        @RequestParam(required = false) queueName: String?,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) startTime: LocalDateTime,
        @RequestParam @DateTimeFormat(iso = DateTimeFormat.ISO.DATE_TIME) endTime: LocalDateTime
    ): Mono<ApiResponse<TraceStatistics>> {
        logger.info("추적 통계 조회 요청: queueName=$queueName, period=$startTime~$endTime")
        
        val startInstant = startTime.toInstant(ZoneOffset.UTC)
        val endInstant = endTime.toInstant(ZoneOffset.UTC)
        
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(Mono.error(AuthenticationException("Invalid API key")))
            .flatMap { tenantId ->
                messageTraceService.getTraceStatistics(tenantId, queueName, startInstant, endInstant)
                    .map { statistics ->
                        logger.info("추적 통계 조회 완료: totalMessages=${statistics.totalMessages}")
                        ApiResponse.success(statistics)
                    }
            }
    }
}
