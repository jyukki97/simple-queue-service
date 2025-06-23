package com.example.sqs.performance.test

import com.example.sqs.message.domain.ReceiveMessageResponse
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.service.MessageService
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.service.QueueService
import com.example.sqs.test.base.BaseServiceTest
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.DisplayName
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import reactor.core.publisher.Flux
import reactor.test.StepVerifier
import java.util.concurrent.atomic.AtomicLong
import kotlin.system.measureTimeMillis

/**
 * 성능 테스트 클래스
 */
@Disabled("성능 테스트는 필요시에만 실행")
class PerformanceTest : BaseServiceTest() {

    @Autowired
    private lateinit var messageService: MessageService

    @Autowired
    private lateinit var queueService: QueueService

    @Test
    @DisplayName("메시지 전송 처리량 테스트 - 1000개 메시지")
    fun testMessageSendThroughput() {
        // Given
        val tenantId = "perf-test-tenant"
        val queueName = "perf-test-queue"
        val messageCount = 1000
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // 큐 생성
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        ).expectNextCount(1).verifyComplete()

        // When
        val successCount = AtomicLong(0)
        val executionTime = measureTimeMillis {
            StepVerifier.create(
                Flux.range(1, messageCount)
                    .flatMap({ index ->
                        val request = SendMessageRequest(
                            messageBody = "Performance test message $index",
                            delaySeconds = 0,
                            attributes = mapOf("index" to index.toString())
                        )
                        messageService.sendMessage(tenantId, queueName, request)
                            .doOnNext { successCount.incrementAndGet() }
                    }, 50) // 동시성 50
            )
                .expectNextCount(messageCount.toLong())
                .verifyComplete()
        }

        // Then
        val throughput = messageCount.toDouble() / (executionTime / 1000.0)
        println("메시지 전송 성능:")
        println("- 총 메시지 수: $messageCount")
        println("- 실행 시간: ${executionTime}ms")
        println("- 처리량: ${String.format("%.2f", throughput)} messages/sec")
        println("- 성공률: ${(successCount.get().toDouble() / messageCount * 100).toInt()}%")

        assert(throughput > 100) { "처리량이 100 msg/sec 미만입니다: $throughput" }
    }

    @Test
    @DisplayName("메시지 수신 처리량 테스트")
    fun testMessageReceiveThroughput() {
        // Given
        val tenantId = "perf-test-tenant"
        val queueName = "perf-receive-queue"
        val messageCount = 500
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // 큐 생성 및 메시지 전송
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
                .thenMany(
                    Flux.range(1, messageCount)
                        .flatMap { index ->
                            val request = SendMessageRequest(
                                messageBody = "Receive test message $index",
                                delaySeconds = 0,
                                attributes = mapOf()
                            )
                            messageService.sendMessage(tenantId, queueName, request)
                        }
                )
        ).expectNextCount(messageCount.toLong()).verifyComplete()

        // When - 메시지 수신
        val receiveCount = AtomicLong(0)
        val executionTime = measureTimeMillis {
            StepVerifier.create(
                Flux.range(1, messageCount / 10) // 10개씩 배치로 수신
                    .flatMap {
                        val receiveRequest = com.example.sqs.message.domain.ReceiveMessageRequest(
                            maxNumberOfMessages = 10,
                            visibilityTimeoutSeconds = 30,
                            waitTimeSeconds = 0
                        )
                        messageService.receiveMessages(tenantId, queueName, receiveRequest)
                            .doOnNext { messageResponse ->
                                receiveCount.addAndGet(1) // 각 메시지마다 카운트 증가
                            }
                    }
            )
                .expectNextCount((messageCount / 10).toLong())
                .verifyComplete()
        }

        // Then
        val throughput = receiveCount.get().toDouble() / (executionTime / 1000.0)
        println("메시지 수신 성능:")
        println("- 총 수신 메시지 수: ${receiveCount.get()}")
        println("- 실행 시간: ${executionTime}ms")
        println("- 처리량: ${String.format("%.2f", throughput)} messages/sec")

        assert(throughput > 50) { "수신 처리량이 50 msg/sec 미만입니다: $throughput" }
    }

    @Test
    @DisplayName("동시성 테스트 - 여러 스레드에서 동시 메시지 처리")
    fun testConcurrentMessageProcessing() {
        // Given
        val tenantId = "concurrent-test-tenant"
        val queueName = "concurrent-test-queue"
        val messageCount = 1000
        val concurrency = 20
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // 큐 생성
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        ).expectNextCount(1).verifyComplete()

        // When
        val successCount = AtomicLong(0)
        val errorCount = AtomicLong(0)

        val executionTime = measureTimeMillis {
            StepVerifier.create(
                Flux.range(1, messageCount)
                    .flatMap({ index ->
                        val request = SendMessageRequest(
                            messageBody = "Concurrent test message $index",
                            delaySeconds = 0,
                            attributes = mapOf("thread" to Thread.currentThread().name)
                        )
                        messageService.sendMessage(tenantId, queueName, request)
                            .doOnNext { successCount.incrementAndGet() }
                            .doOnError { errorCount.incrementAndGet() }
                            .onErrorComplete()
                    }, concurrency)
            )
                .expectNextCount(messageCount.toLong())
                .verifyComplete()
        }

        // Then
        println("동시성 테스트 결과:")
        println("- 총 메시지 수: $messageCount")
        println("- 동시성 레벨: $concurrency")
        println("- 실행 시간: ${executionTime}ms")
        println("- 성공: ${successCount.get()}")
        println("- 실패: ${errorCount.get()}")
        println("- 성공률: ${(successCount.get().toDouble() / messageCount * 100).toInt()}%")

        assert(successCount.get() >= messageCount * 0.95) { "성공률이 95% 미만입니다" }
    }

    @Test
    @DisplayName("메모리 사용량 테스트")
    fun testMemoryUsage() {
        // Given
        val tenantId = "memory-test-tenant"
        val queueName = "memory-test-queue"
        val messageCount = 10000
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // 큐 생성
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        ).expectNextCount(1).verifyComplete()

        // When
        val runtime = Runtime.getRuntime()
        val initialMemory = runtime.totalMemory() - runtime.freeMemory()

        StepVerifier.create(
            Flux.range(1, messageCount)
                .flatMap({ index ->
                    val request = SendMessageRequest(
                        messageBody = "Memory test message $index with some additional content to increase memory usage",
                        delaySeconds = 0,
                        attributes = mapOf("index" to index.toString())
                    )
                    messageService.sendMessage(tenantId, queueName, request)
                }, 10)
        ).expectNextCount(messageCount.toLong()).verifyComplete()

        System.gc() // 가비지 컬렉션 실행
        Thread.sleep(1000) // GC 완료 대기

        val finalMemory = runtime.totalMemory() - runtime.freeMemory()
        val memoryIncrease = finalMemory - initialMemory

        // Then
        println("메모리 사용량 테스트 결과:")
        println("- 초기 메모리: ${initialMemory / 1024 / 1024}MB")
        println("- 최종 메모리: ${finalMemory / 1024 / 1024}MB")
        println("- 메모리 증가: ${memoryIncrease / 1024 / 1024}MB")
        println("- 메시지당 메모리: ${memoryIncrease / messageCount}bytes")

        // 메시지당 메모리 사용량이 적절한지 확인
        assert(memoryIncrease / messageCount < 1000) { "메시지당 메모리 사용량이 너무 큽니다" }
    }

    @Test
    @DisplayName("응답 시간 분포 테스트")
    fun testResponseTimeDistribution() {
        // Given
        val tenantId = "response-time-tenant"
        val queueName = "response-time-queue"
        val messageCount = 100
        val attributes = QueueAttributes(
            visibilityTimeoutSeconds = 30,
            messageRetentionPeriod = 1209600,
            maxReceiveCount = 3
        )

        // 큐 생성
        StepVerifier.create(
            queueService.createQueue(tenantId, queueName, attributes)
        ).expectNextCount(1).verifyComplete()

        // When
        val responseTimes = mutableListOf<Long>()

        (1..messageCount).forEach { index ->
            val request = SendMessageRequest(
                messageBody = "Response time test message $index",
                delaySeconds = 0,
                attributes = mapOf()
            )

            val responseTime = measureTimeMillis {
                StepVerifier.create(
                    messageService.sendMessage(tenantId, queueName, request)
                ).expectNextCount(1).verifyComplete()
            }

            responseTimes.add(responseTime)
        }

        // Then
        val sortedTimes = responseTimes.sorted()
        val p50 = sortedTimes[sortedTimes.size / 2]
        val p95 = sortedTimes[(sortedTimes.size * 0.95).toInt()]
        val p99 = sortedTimes[(sortedTimes.size * 0.99).toInt()]
        val average = responseTimes.average()

        println("응답 시간 분포:")
        println("- 평균: ${String.format("%.2f", average)}ms")
        println("- P50: ${p50}ms")
        println("- P95: ${p95}ms")
        println("- P99: ${p99}ms")
        println("- 최대: ${sortedTimes.last()}ms")

        assert(p95 < 1000) { "P95 응답 시간이 1초를 초과합니다: ${p95}ms" }
        assert(average < 100) { "평균 응답 시간이 100ms를 초과합니다: $average ms" }
    }
}
