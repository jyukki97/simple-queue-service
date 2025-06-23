package com.example.sqs.metrics.controller

import com.example.sqs.metrics.domain.*
import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.service.TenantService
import org.junit.jupiter.api.BeforeEach
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.autoconfigure.web.reactive.AutoConfigureWebTestClient
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.http.MediaType
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.test.StepVerifier
import java.time.Duration
import java.time.Instant

@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@AutoConfigureWebTestClient
@org.springframework.test.context.ActiveProfiles("test")
class MetricsControllerIntegrationTest {

    @Autowired
    lateinit var webTestClient: WebTestClient

    @Autowired
    lateinit var tenantService: TenantService

    private lateinit var testTenant: Tenant
    private val testApiKey = "test-api-key-metrics"

    @BeforeEach
    fun setUp() {
        testTenant = tenantService.createTenant("test-tenant-metrics", testApiKey, "메트릭 테스트 테넌트").block()!!
    }

    @Test
    fun `큐 메트릭 조회 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/queues/test-queue")
            .header("X-API-Key", testApiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.queueName").isEqualTo("test-queue")
            .jsonPath("$.data.tenantId").isEqualTo(testTenant.tenantId)
            .jsonPath("$.data.messagesAvailable").isNumber
            .jsonPath("$.data.messagesInFlight").isNumber
            .jsonPath("$.data.messagesSentPerMinute").isNumber
            .jsonPath("$.data.errorRate").isNumber
    }

    @Test
    fun `시스템 메트릭 조회 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/system")
            .header("X-API-Key", testApiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.totalQueues").isNumber
            .jsonPath("$.data.totalMessages").isNumber
            .jsonPath("$.data.totalThroughputPerSecond").isNumber
            .jsonPath("$.data.errorRate").isNumber
            .jsonPath("$.data.memoryUsageBytes").isNumber
            .jsonPath("$.data.cpuUsagePercent").isNumber
    }

    @Test
    fun `모든 큐 메트릭 조회 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/queues")
            .header("X-API-Key", testApiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").isArray
    }

    @Test
    fun `알림 규칙 생성 테스트`() {
        val createRequest = CreateAlertRuleRequest(
            name = "테스트 알림 규칙",
            description = "테스트용 알림 규칙입니다",
            metricName = "messages_available",
            condition = AlertCondition.GREATER_THAN,
            threshold = 100.0,
            duration = 300
        )

        webTestClient.post()
            .uri("/api/v1/metrics/alerts/rules")
            .header("X-API-Key", testApiKey)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.name").isEqualTo("테스트 알림 규칙")
            .jsonPath("$.data.metricName").isEqualTo("messages_available")
            .jsonPath("$.data.condition").isEqualTo("GREATER_THAN")
            .jsonPath("$.data.threshold").isEqualTo(100.0)
            .jsonPath("$.data.isEnabled").isEqualTo(true)
    }

    @Test
    fun `알림 규칙 목록 조회 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/alerts/rules")
            .header("X-API-Key", testApiKey)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data").isArray
            .jsonPath("$.data.length()").value { count: Int ->
                assert(count >= 4) // 기본 규칙 4개 이상
            }
    }

    @Test
    fun `수동 알림 트리거 테스트`() {
        val triggerRequest = TriggerAlertRequest(
            ruleId = "queue-size-high",
            message = "테스트 알림 메시지",
            severity = AlertSeverity.WARNING,
            metricValue = 1500.0,
            queueName = "test-queue"
        )

        webTestClient.post()
            .uri("/api/v1/metrics/alerts/trigger")
            .header("X-API-Key", testApiKey)
            .contentType(MediaType.APPLICATION_JSON)
            .bodyValue(triggerRequest)
            .exchange()
            .expectStatus().isOk
            .expectBody()
            .jsonPath("$.success").isEqualTo(true)
            .jsonPath("$.data.ruleId").isEqualTo("queue-size-high")
            .jsonPath("$.data.severity").isEqualTo("WARNING")
            .jsonPath("$.data.metricValue").isEqualTo(1500.0)
            .jsonPath("$.data.queueName").isEqualTo("test-queue")
    }

    @Test
    fun `잘못된 API 키로 메트릭 조회 실패 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/queues/test-queue")
            .header("X-API-Key", "invalid-api-key")
            .exchange()
            .expectStatus().is4xxClientError // 4xx 에러면 충분
    }

    @Test
    fun `API 키 없이 메트릭 조회 실패 테스트`() {
        webTestClient.get()
            .uri("/api/v1/metrics/queues/test-queue")
            .exchange()
            .expectStatus().isUnauthorized
    }
}
