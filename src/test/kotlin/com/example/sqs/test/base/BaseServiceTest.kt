package com.example.sqs.test.base

import com.example.sqs.test.util.TestDataFactory
import org.junit.jupiter.api.AfterEach
import org.junit.jupiter.api.BeforeEach
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.test.context.ActiveProfiles
import reactor.test.StepVerifier
import java.io.File

/**
 * 서비스 테스트 기반 클래스 - 성능 테스트용
 */
@SpringBootTest(webEnvironment = SpringBootTest.WebEnvironment.NONE)
@ActiveProfiles("test")
abstract class BaseServiceTest {
    
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
        
        // 테스트 데이터 디렉토리 정리
        try {
            val testDataDir = File("./test-data")
            if (testDataDir.exists()) {
                testDataDir.deleteRecursively()
            }
            val dataDir = File("./data")
            if (dataDir.exists()) {
                dataDir.deleteRecursively()
            }
        } catch (e: Exception) {
            // 파일 정리 실패 시 무시
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
            val dataDir = File("./data")
            if (dataDir.exists()) {
                dataDir.deleteRecursively()
            }
        } catch (e: Exception) {
            // 파일 정리 실패 시 무시
        }
    }
}
