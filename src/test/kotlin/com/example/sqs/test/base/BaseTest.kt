package com.example.sqs.test.base

import com.example.sqs.test.util.TestDataFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.web.reactive.server.WebTestClient
import reactor.test.StepVerifier
import java.io.File
import java.time.Duration

/**
 * 통합 테스트 기반 클래스
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class BaseIntegrationTest {
    
    @Autowired
    protected lateinit var webTestClient: WebTestClient
    
    @Autowired
    protected lateinit var redisTemplate: ReactiveRedisTemplate<String, String>
    
    protected val testDataFactory = TestDataFactory
    
    @BeforeEach
    fun baseSetUp() {
        // Redis 데이터 정리
        try {
            StepVerifier.create(redisTemplate.execute { connection ->
                connection.serverCommands().flushAll()
            }).expectNextMatches { it == "OK" }.verifyComplete()
        } catch (e: Exception) {
            // Redis 연결 실패 시 무시 (테스트 환경에서는 선택사항)
        }
    }
    
    @AfterEach
    fun baseTearDown() {
        // 테스트 데이터 디렉토리 정리
        try {
            val testDataDir = File("./test-data")
            if (testDataDir.exists()) {
                testDataDir.deleteRecursively()
            }
        } catch (e: Exception) {
            // 정리 실패 시 무시
        }
    }
    
    /**
     * 테스트용 헬퍼 메서드들
     */
    protected fun waitForAsync(duration: Duration = Duration.ofMillis(100)) {
        Thread.sleep(duration.toMillis())
    }
    
    protected fun createTestApiKey(): String = "test-api-key-${System.currentTimeMillis()}"
    
    protected fun createTestTenantId(): String = "test-tenant-${System.currentTimeMillis()}"
}

/**
 * 컨트롤러 테스트 기반 클래스
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.RANDOM_PORT)
@ActiveProfiles("test")
abstract class BaseControllerTest {
    
    @Autowired
    protected lateinit var webTestClient: WebTestClient
    
    protected val testDataFactory = TestDataFactory
    
    protected val testApiKey = "test-api-key"
    protected val testTenantId = "test-tenant"
    
    /**
     * 공통 HTTP 헤더 설정
     */
    protected fun WebTestClient.RequestHeadersSpec<*>.withTestHeaders(): WebTestClient.RequestHeadersSpec<*> {
        return this.header("X-API-Key", testApiKey)
            .header("X-Tenant-Id", testTenantId)
    }
}
