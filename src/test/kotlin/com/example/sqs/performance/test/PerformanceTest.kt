package com.example.sqs.performance.test

import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.queue.domain.CreateQueueRequest
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.test.base.BaseIntegrationTest
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.test.context.TestPropertySource
import kotlin.system.measureTimeMillis

/**
 * 성능 테스트 클래스
 */
@Disabled
@TestPropertySource(
    properties = [
        "sqs.storage.warm.max-size=1000000",
        "sqs.storage.cold.enabled=true",
        "sqs.storage.tier.enabled=true",
        "app.metrics.enable-prometheus=false"
    ]
)
class PerformanceTest : BaseIntegrationTest() {

    private val logger = LoggerFactory.getLogger(PerformanceTest::class.java)

    @Test
    fun `test high volume message send performance`() = runBlocking {
        logger.info("Starting high volume message send performance test")

        // Standard 큐 생성
        val queueName = "perf-test-standard"
        val createRequest = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated

        val messageCount = 1000
        val batchSize = 100
        val batches = messageCount / batchSize

        val totalTime = measureTimeMillis {
            repeat(batches) { batchIndex ->
                val batchTime = measureTimeMillis {
                    repeat(batchSize) { messageIndex ->
                        val messageNumber = batchIndex * batchSize + messageIndex
                        val messageRequest = SendMessageRequest(
                            messageBody = "Performance test message #$messageNumber - ${System.currentTimeMillis()}",
                            delaySeconds = 0,
                            attributes = mapOf(
                                "TestId" to "perf-test",
                                "BatchIndex" to batchIndex.toString(),
                                "MessageIndex" to messageIndex.toString()
                            )
                        )

                        webTestClient.post()
                            .uri("/api/v1/queues/$queueName/messages")
                            .header("X-API-Key", "test-api-key")
                            .bodyValue(messageRequest)
                            .exchange()
                            .expectStatus().isCreated
                    }
                }
                logger.info("Batch $batchIndex completed in ${batchTime}ms (${batchSize * 1000 / batchTime} msg/sec)")
            }
        }

        val messagesPerSecond = messageCount * 1000 / totalTime
        logger.info("High volume send test completed:")
        logger.info("- Messages sent: $messageCount")
        logger.info("- Total time: ${totalTime}ms")
        logger.info("- Throughput: $messagesPerSecond messages/second")

        // 성능 임계값 검증 (예: 최소 50 msg/sec)
        assert(messagesPerSecond >= 50) {
            "Send throughput too low: $messagesPerSecond msg/sec (expected >= 50)"
        }
    }

    @Test
    fun `test high volume message receive performance`() = runBlocking {
        logger.info("Starting high volume message receive performance test")

        // FIFO 큐 생성
        val queueName = "perf-test-fifo.fifo"
        val createRequest = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3,
                fifoQueue = true
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated

        // 먼저 메시지들을 전송
        val messageCount = 500
        repeat(messageCount) { index ->
            val messageRequest = SendMessageRequest(
                messageBody = "Receive test message #$index",
                delaySeconds = 0,
                attributes = mapOf("index" to index.toString()),
                messageGroupId = "group-${index % 10}",
                messageDeduplicationId = "dedup-$index"
            )

            webTestClient.post()
                .uri("/api/v1/queues/$queueName/messages")
                .header("X-API-Key", "test-api-key")
                .bodyValue(messageRequest)
                .exchange()
                .expectStatus().isCreated
        }

        // 메시지 수신 성능 테스트
        var receivedCount = 0
        val totalTime = measureTimeMillis {
            while (receivedCount < messageCount) {
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$queueName/messages?maxNumberOfMessages=10&waitTimeSeconds=1&visibilityTimeoutSeconds=30")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                // 응답에서 메시지 개수 파싱 (실제 구현에 따라 조정 필요)
                if (response?.contains("\"messages\"") == true) {
                    receivedCount += 10.coerceAtMost(messageCount - receivedCount)
                } else {
                    // 더 이상 메시지가 없으면 종료
                    return@measureTimeMillis
                }
            }
        }

        val messagesPerSecond = receivedCount * 1000 / totalTime
        logger.info("High volume receive test completed:")
        logger.info("- Messages received: $receivedCount")
        logger.info("- Total time: ${totalTime}ms")
        logger.info("- Throughput: $messagesPerSecond messages/second")

        // 성능 임계값 검증
        assert(messagesPerSecond >= 30) {
            "Receive throughput too low: $messagesPerSecond msg/sec (expected >= 30)"
        }
    }

    @Test
    fun `test tiered storage performance`() = runBlocking {
        logger.info("Starting tiered storage performance test")

        // 큐 생성
        val queueName = "perf-test-tiering"
        val createRequest = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated

        // WARM 스토리지 용량을 초과하는 메시지 전송
        val messageCount = 200
        val sendTime = measureTimeMillis {
            repeat(messageCount) { index ->
                val largeMessage = "Large message #$index - " + "x".repeat(1000)
                val messageRequest = SendMessageRequest(
                    messageBody = largeMessage,
                    delaySeconds = 0,
                    attributes = mapOf(
                        "MessageSize" to largeMessage.length.toString(),
                        "TestType" to "tiering"
                    )
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$queueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        logger.info("Tiered storage - Send phase completed in ${sendTime}ms")

        // 잠시 대기하여 티어링이 발생하도록 함
        Thread.sleep(2000)

        // 메시지 수신 성능 (티어링된 메시지 포함)
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            repeat(40) { // 최대 40번 시도
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$queueName/messages?maxNumberOfMessages=5&waitTimeSeconds=1")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    receivedCount += 5
                } else {
                    return@measureTimeMillis
                }
            }
        }

        val receiveMessagesPerSecond = receivedCount * 1000 / receiveTime
        logger.info("Tiered storage test completed:")
        logger.info("- Messages sent: $messageCount")
        logger.info("- Messages received: $receivedCount")
        logger.info("- Send time: ${sendTime}ms")
        logger.info("- Receive time: ${receiveTime}ms")
        logger.info("- Receive throughput: $receiveMessagesPerSecond messages/second")

        // 티어링 환경에서도 적절한 성능이 나와야 함
        assert(receiveMessagesPerSecond >= 20) {
            "Tiered receive throughput too low: $receiveMessagesPerSecond msg/sec (expected >= 20)"
        }
    }

    @Test
    fun `compare FIFO vs Standard queue performance`() = runBlocking {
        logger.info("Starting FIFO vs Standard queue performance comparison")

        // Standard 큐 생성
        val standardQueueName = "perf-compare-standard"
        val standardCreateRequest = CreateQueueRequest(
            queueName = standardQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(standardCreateRequest)
            .exchange()
            .expectStatus().isCreated

        // FIFO 큐 생성
        val fifoQueueName = "perf-compare-fifo.fifo"
        val fifoCreateRequest = CreateQueueRequest(
            queueName = fifoQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3,
                fifoQueue = true
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(fifoCreateRequest)
            .exchange()
            .expectStatus().isCreated

        val messageCount = 100

        // Standard 큐 성능 테스트
        val standardTime = measureTimeMillis {
            repeat(messageCount) { index ->
                val messageRequest = SendMessageRequest(
                    messageBody = "Standard message #$index",
                    delaySeconds = 0,
                    attributes = mapOf("test" to "standard")
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$standardQueueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        // FIFO 큐 성능 테스트
        val fifoTime = measureTimeMillis {
            repeat(messageCount) { index ->
                val messageRequest = SendMessageRequest(
                    messageBody = "FIFO message #$index",
                    delaySeconds = 0,
                    attributes = mapOf("test" to "fifo"),
                    messageGroupId = "group-${index % 5}",
                    messageDeduplicationId = "dedup-$index"
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$fifoQueueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        val standardThroughput = messageCount * 1000 / standardTime
        val fifoThroughput = messageCount * 1000 / fifoTime

        logger.info("Queue performance comparison:")
        logger.info("- Standard queue: ${standardTime}ms ($standardThroughput msg/sec)")
        logger.info("- FIFO queue: ${fifoTime}ms ($fifoThroughput msg/sec)")
        logger.info("- Performance ratio (Standard/FIFO): ${standardThroughput.toDouble() / fifoThroughput}")

        // 두 큐 모두 최소 성능 기준을 만족해야 함
        assert(standardThroughput >= 30) {
            "Standard queue throughput too low: $standardThroughput msg/sec"
        }
        assert(fifoThroughput >= 20) {
            "FIFO queue throughput too low: $fifoThroughput msg/sec"
        }
    }

    @Test
    fun `test cold storage direct performance`() = runBlocking {
        logger.info("Starting cold storage direct performance test")

        // 큐 생성
        val queueName = "perf-test-cold-direct"
        val createRequest = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated

        // WARM 스토리지 용량을 강제로 초과시켜 COLD로 직접 저장되도록 함
        val messageCount = 300
        val largeMessageSize = 5000 // 5KB per message

        val sendTime = measureTimeMillis {
            repeat(messageCount) { index ->
                val largeMessage = "Cold storage test message #$index - " + "x".repeat(largeMessageSize)
                val messageRequest = SendMessageRequest(
                    messageBody = largeMessage,
                    delaySeconds = 0,
                    attributes = mapOf(
                        "MessageSize" to largeMessage.length.toString(),
                        "TestType" to "cold-direct",
                        "MessageIndex" to index.toString()
                    )
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$queueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        logger.info("Cold storage direct - Send phase completed in ${sendTime}ms")

        // 충분한 대기 시간으로 COLD 스토리지로 완전 이동 보장
        Thread.sleep(5000)

        // COLD 스토리지에서 메시지 수신 성능 테스트
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            var consecutiveEmptyResponses = 0
            repeat(60) { // 최대 60번 시도
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$queueName/messages?maxNumberOfMessages=5&waitTimeSeconds=2")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    receivedCount += 5
                    consecutiveEmptyResponses = 0
                    logger.info("Cold storage receive batch: $receivedCount messages received so far")
                } else {
                    consecutiveEmptyResponses++
                    logger.info("No messages in this batch (consecutive empty: $consecutiveEmptyResponses)")
                    if (consecutiveEmptyResponses >= 3) {
                        logger.info("No more messages in cold storage - stopping early")
                        return@repeat
                    }
                }
            }
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("Cold storage direct test completed:")
        logger.info("- Messages sent: $messageCount")
        logger.info("- Messages received: $receivedCount")
        logger.info("- Send time: ${sendTime}ms")
        logger.info("- Receive time: ${receiveTime}ms")
        logger.info("- Send throughput: $sendThroughput messages/second")
        logger.info("- Receive throughput: $receiveThroughput messages/second")

        // COLD 스토리지 성능 기준 (WARM보다 낮은 기준)
        assert(sendThroughput >= 10) {
            "Cold storage send throughput too low: $sendThroughput msg/sec (expected >= 10)"
        }
        assert(receiveThroughput >= 5) {
            "Cold storage receive throughput too low: $receiveThroughput msg/sec (expected >= 5)"
        }
    }

    @Test
    fun `test storage tier performance comparison`() = runBlocking {
        logger.info("Starting storage tier performance comparison test")

        // 1. HOT 스토리지 성능 (Redis 전용)
        val hotQueueName = "perf-compare-hot"
        val hotCreateRequest = CreateQueueRequest(
            queueName = hotQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(hotCreateRequest)
            .exchange()
            .expectStatus().isCreated

        // 2. WARM 스토리지 성능 (RocksDB)
        val warmQueueName = "perf-compare-warm"
        val warmCreateRequest = CreateQueueRequest(
            queueName = warmQueueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(warmCreateRequest)
            .exchange()
            .expectStatus().isCreated

        val testMessageCount = 50
        val mediumMessageSize = 1000 // 1KB

        // HOT 스토리지 테스트 (작은 메시지, Redis에 유지)
        val hotSendTime = measureTimeMillis {
            repeat(testMessageCount) { index ->
                val message = "Hot message #$index - " + "x".repeat(100) // 작은 메시지
                val messageRequest = SendMessageRequest(
                    messageBody = message,
                    delaySeconds = 0,
                    attributes = mapOf("tier" to "hot", "index" to index.toString())
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$hotQueueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        // WARM 스토리지 테스트 (중간 크기 메시지)
        val warmSendTime = measureTimeMillis {
            repeat(testMessageCount) { index ->
                val message = "Warm message #$index - " + "x".repeat(mediumMessageSize)
                val messageRequest = SendMessageRequest(
                    messageBody = message,
                    delaySeconds = 0,
                    attributes = mapOf("tier" to "warm", "index" to index.toString())
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$warmQueueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        // 약간의 대기 시간
        Thread.sleep(1000)

        // HOT 스토리지 수신 테스트
        var hotReceived = 0
        val hotReceiveTime = measureTimeMillis {
            var consecutiveEmptyResponses = 0
            repeat(10) {
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$hotQueueName/messages?maxNumberOfMessages=5&waitTimeSeconds=1")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    hotReceived += 5
                    consecutiveEmptyResponses = 0
                } else {
                    consecutiveEmptyResponses++
                    if (consecutiveEmptyResponses >= 2) {
                        // 연속으로 2번 빈 응답이면 조기 종료
                        return@repeat
                    }
                }
            }
        }

        // WARM 스토리지 수신 테스트
        var warmReceived = 0
        val warmReceiveTime = measureTimeMillis {
            var consecutiveEmptyResponses = 0
            repeat(10) {
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$warmQueueName/messages?maxNumberOfMessages=5&waitTimeSeconds=1")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    warmReceived += 5
                    consecutiveEmptyResponses = 0
                } else {
                    consecutiveEmptyResponses++
                    if (consecutiveEmptyResponses >= 2) {
                        // 연속으로 2번 빈 응답이면 조기 종료
                        return@repeat
                    }
                }
            }
        }

        // 성능 계산
        val hotSendThroughput = testMessageCount * 1000 / hotSendTime
        val warmSendThroughput = testMessageCount * 1000 / warmSendTime
        val hotReceiveThroughput = hotReceived * 1000 / hotReceiveTime
        val warmReceiveThroughput = warmReceived * 1000 / warmReceiveTime

        logger.info("Storage tier performance comparison:")
        logger.info("HOT Storage (Redis):")
        logger.info("- Send: ${hotSendTime}ms ($hotSendThroughput msg/sec)")
        logger.info("- Receive: ${hotReceiveTime}ms ($hotReceiveThroughput msg/sec)")
        logger.info("WARM Storage (RocksDB):")
        logger.info("- Send: ${warmSendTime}ms ($warmSendThroughput msg/sec)")
        logger.info("- Receive: ${warmReceiveTime}ms ($warmReceiveThroughput msg/sec)")
        logger.info("Performance Ratios:")
        logger.info("- Send ratio (HOT/WARM): ${hotSendThroughput.toDouble() / warmSendThroughput}")
        logger.info("- Receive ratio (HOT/WARM): ${hotReceiveThroughput.toDouble() / warmReceiveThroughput}")

        // 계층별 최소 성능 기준 (현실적인 수치로 조정)
        assert(hotSendThroughput >= 80) {
            "HOT storage send throughput too low: $hotSendThroughput msg/sec (expected >= 80)"
        }
        assert(hotReceiveThroughput >= 150) {
            "HOT storage receive throughput too low: $hotReceiveThroughput msg/sec (expected >= 150)"
        }
        assert(warmSendThroughput >= 40) {
            "WARM storage send throughput too low: $warmSendThroughput msg/sec (expected >= 40)"
        }
        assert(warmReceiveThroughput >= 80) {
            "WARM storage receive throughput too low: $warmReceiveThroughput msg/sec (expected >= 80)"
        }
    }

    @Test
    fun `test storage tier transition performance`() = runBlocking {
        logger.info("Starting storage tier transition performance test")

        val queueName = "perf-test-transition"
        val createRequest = CreateQueueRequest(
            queueName = queueName,
            attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600,
                delaySeconds = 0,
                maxReceiveCount = 3
            )
        )

        webTestClient.post()
            .uri("/api/v1/queues")
            .header("X-API-Key", "test-api-key")
            .bodyValue(createRequest)
            .exchange()
            .expectStatus().isCreated

        // Phase 1: HOT → WARM 전환 테스트
        logger.info("Phase 1: Testing HOT → WARM transition")
        val phase1MessageCount = 100
        val phase1SendTime = measureTimeMillis {
            repeat(phase1MessageCount) { index ->
                val message = "Transition message #$index - " + "x".repeat(500)
                val messageRequest = SendMessageRequest(
                    messageBody = message,
                    delaySeconds = 0,
                    attributes = mapOf(
                        "phase" to "hot-to-warm",
                        "index" to index.toString()
                    )
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$queueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        // 즉시 수신 테스트 (HOT에서 수신)
        var phase1Received = 0
        val phase1ReceiveTime = measureTimeMillis {
            var consecutiveEmptyResponses = 0
            repeat(20) {
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$queueName/messages?maxNumberOfMessages=5&waitTimeSeconds=1")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    phase1Received += 5
                    consecutiveEmptyResponses = 0
                } else {
                    consecutiveEmptyResponses++
                    if (consecutiveEmptyResponses >= 3) {
                        // 연속으로 3번 빈 응답이면 조기 종료
                        return@repeat
                    }
                }
            }
        }

        // Phase 2: WARM → COLD 전환 테스트
        logger.info("Phase 2: Testing WARM → COLD transition")
        val phase2MessageCount = 150
        val phase2SendTime = measureTimeMillis {
            repeat(phase2MessageCount) { index ->
                val message = "Large transition message #$index - " + "x".repeat(2000) // 2KB
                val messageRequest = SendMessageRequest(
                    messageBody = message,
                    delaySeconds = 0,
                    attributes = mapOf(
                        "phase" to "warm-to-cold",
                        "index" to index.toString()
                    )
                )

                webTestClient.post()
                    .uri("/api/v1/queues/$queueName/messages")
                    .header("X-API-Key", "test-api-key")
                    .bodyValue(messageRequest)
                    .exchange()
                    .expectStatus().isCreated
            }
        }

        // 티어링 대기
        Thread.sleep(3000)

        // 전환 후 수신 테스트 (WARM/COLD 혼합)
        var phase2Received = 0
        val phase2ReceiveTime = measureTimeMillis {
            var consecutiveEmptyResponses = 0
            repeat(30) {
                val response = webTestClient.get()
                    .uri("/api/v1/queues/$queueName/messages?maxNumberOfMessages=5&waitTimeSeconds=2")
                    .header("X-API-Key", "test-api-key")
                    .exchange()
                    .expectStatus().isOk
                    .returnResult(String::class.java)
                    .responseBody
                    .blockFirst()

                if (response?.contains("\"messages\"") == true) {
                    phase2Received += 5
                    consecutiveEmptyResponses = 0
                } else {
                    consecutiveEmptyResponses++
                    if (consecutiveEmptyResponses >= 3) {
                        // 연속으로 3번 빈 응답이면 조기 종료
                        return@repeat
                    }
                }
            }
        }

        // 성능 계산 및 로깅
        val phase1SendThroughput = phase1MessageCount * 1000 / phase1SendTime
        val phase1ReceiveThroughput = phase1Received * 1000 / phase1ReceiveTime
        val phase2SendThroughput = phase2MessageCount * 1000 / phase2SendTime
        val phase2ReceiveThroughput = phase2Received * 1000 / phase2ReceiveTime

        logger.info("Storage tier transition performance results:")
        logger.info("Phase 1 (HOT → WARM):")
        logger.info("- Send: ${phase1SendTime}ms ($phase1SendThroughput msg/sec)")
        logger.info("- Receive: ${phase1ReceiveTime}ms ($phase1ReceiveThroughput msg/sec)")
        logger.info("- Messages received: $phase1Received")
        logger.info("Phase 2 (WARM → COLD):")
        logger.info("- Send: ${phase2SendTime}ms ($phase2SendThroughput msg/sec)")
        logger.info("- Receive: ${phase2ReceiveTime}ms ($phase2ReceiveThroughput msg/sec)")
        logger.info("- Messages received: $phase2Received")

        // 전환 성능 기준
        assert(phase1SendThroughput >= 80) {
            "Phase 1 send throughput too low: $phase1SendThroughput msg/sec (expected >= 80)"
        }
        assert(phase1ReceiveThroughput >= 150) {
            "Phase 1 receive throughput too low: $phase1ReceiveThroughput msg/sec (expected >= 150)"
        }
        assert(phase2SendThroughput >= 40) {
            "Phase 2 send throughput too low: $phase2SendThroughput msg/sec (expected >= 40)"
        }
        assert(phase2ReceiveThroughput >= 50) {
            "Phase 2 receive throughput too low: $phase2ReceiveThroughput msg/sec (expected >= 50)"
        }
    }
}
