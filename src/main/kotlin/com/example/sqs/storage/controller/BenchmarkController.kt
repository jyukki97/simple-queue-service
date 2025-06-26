package com.example.sqs.storage.controller

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.benchmark.ColdStorageBenchmark
import org.springframework.http.ResponseEntity
import org.springframework.web.bind.annotation.*
import java.time.Instant
import java.util.*

/**
 * 스토리지 성능 벤치마크 컨트롤러
 * Phase 2 기능 테스트를 위한 REST API 제공
 */
@RestController
@RequestMapping("/api/benchmark")
class BenchmarkController(
    private val coldStorageBenchmark: ColdStorageBenchmark
) {

    /**
     * Phase 2: 체크섬을 포함한 기본 성능 테스트
     */
    @PostMapping("/basic-with-checksum")
    suspend fun runBasicWithChecksumBenchmark(
        @RequestParam(defaultValue = "1000") messageCount: Int,
        @RequestParam(defaultValue = "test-queue") queueName: String
    ): ResponseEntity<Map<String, Any>> {
        return try {
            // 테스트 메시지 생성
            val messages = generateTestMessages(messageCount, queueName)
            val result = coldStorageBenchmark.measurePhase2Performance(messages)
            ResponseEntity.ok(result.plus("queueName" to queueName))
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(
                mapOf(
                    "error" to "Phase 2 벤치마크 실행 실패",
                    "message" to (e.message ?: "알 수 없는 오류"),
                    "timestamp" to System.currentTimeMillis()
                )
            )
        }
    }

    /**
     * Phase 2: 동적 압축 성능 테스트
     */
    @PostMapping("/compression-dynamic")
    suspend fun runDynamicCompressionBenchmark(
        @RequestParam(defaultValue = "500") messageCount: Int,
        @RequestParam(defaultValue = "compression-test-queue") queueName: String
    ): ResponseEntity<Map<String, Any>> {
        return try {
            // 테스트 메시지 생성
            val messages = generateTestMessages(messageCount, queueName)
            val result = coldStorageBenchmark.testDynamicCompression(messages)
            ResponseEntity.ok(result.plus("queueName" to queueName))
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(
                mapOf(
                    "error" to "동적 압축 테스트 실행 실패",
                    "message" to (e.message ?: "알 수 없는 오류"),
                    "timestamp" to System.currentTimeMillis()
                )
            )
        }
    }

    /**
     * Phase 2: 무결성 검증 상태 조회
     */
    @GetMapping("/integrity-status")
    suspend fun getIntegrityStatus(): ResponseEntity<Map<String, Any>> {
        return try {
            val status = coldStorageBenchmark.getIntegrityStatus()
            ResponseEntity.ok(status)
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(
                mapOf(
                    "error" to "무결성 상태 조회 실패",
                    "message" to (e.message ?: "알 수 없는 오류"),
                    "timestamp" to System.currentTimeMillis()
                )
            )
        }
    }

    /**
     * Phase 2: 백그라운드 무결성 검증 시작
     */
    @PostMapping("/integrity-check/start")
    suspend fun startBackgroundIntegrityCheck(): ResponseEntity<Map<String, Any>> {
        return try {
            coldStorageBenchmark.startBackgroundIntegrityCheck()
            ResponseEntity.ok(
                mapOf(
                    "message" to "백그라운드 무결성 검증이 시작되었습니다",
                    "timestamp" to System.currentTimeMillis()
                )
            )
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(
                mapOf(
                    "error" to "백그라운드 무결성 검증 시작 실패",
                    "message" to (e.message ?: "알 수 없는 오류"),
                    "timestamp" to System.currentTimeMillis()
                )
            )
        }
    }

    /**
     * 기본 벤치마크 (Phase 1 호환성)
     */
    @PostMapping("/basic")
    suspend fun runBasicBenchmark(
        @RequestParam(defaultValue = "1000") messageCount: Int,
        @RequestParam(defaultValue = "benchmark-queue") queueName: String
    ): ResponseEntity<Map<String, Any>> {
        return try {
            // Phase 1 기본 성능 측정 (체크섬 없이)
            val startTime = System.currentTimeMillis()
            val messages = generateTestMessages(messageCount, queueName)
            val result = coldStorageBenchmark.measurePhase2Performance(messages)
            val endTime = System.currentTimeMillis()
            
            val basicResult = mapOf(
                "messageCount" to messageCount,
                "queueName" to queueName,
                "totalTimeMs" to (endTime - startTime),
                "avgTimePerMessage" to ((endTime - startTime).toDouble() / messageCount),
                "phase" to "Phase 1 Compatible",
                "timestamp" to System.currentTimeMillis()
            )
            
            ResponseEntity.ok(basicResult)
        } catch (e: Exception) {
            ResponseEntity.internalServerError().body(
                mapOf(
                    "error" to "기본 벤치마크 실행 실패",
                    "message" to (e.message ?: "알 수 없는 오류"),
                    "timestamp" to System.currentTimeMillis()
                )
            )
        }
    }

    /**
     * 테스트용 메시지 생성 헬퍼 함수
     */
    private fun generateTestMessages(count: Int, queueName: String): List<Message> {
        return (1..count).map { i ->
            Message(
                messageId = "test-msg-${UUID.randomUUID()}",
                tenantId = "test-tenant",
                queueName = queueName,
                messageBody = "Test message body $i for Phase 2 benchmark testing",
                sentTimestamp = Instant.now().minusSeconds((count - i).toLong()),
                attributes = mapOf(
                    "testIndex" to i.toString(),
                    "phase" to "2",
                    "benchmark" to "true"
                )
            )
        }
    }
}
