package com.example.sqs.performance.test

import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.service.MessageService
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.service.QueueService
import kotlinx.coroutines.async
import kotlinx.coroutines.awaitAll
import kotlinx.coroutines.delay
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Disabled
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import kotlin.system.measureTimeMillis

@Disabled
@SpringBootTest
@ActiveProfiles("test")
class RecoveryRateTest {

    @Autowired
    private lateinit var queueService: QueueService

    @Autowired
    private lateinit var messageService: MessageService

    private val logger = LoggerFactory.getLogger(RecoveryRateTest::class.java)

    @Test
    fun `recovery rate diagnosis test`() = runBlocking {
        logger.info("=== Recovery Rate Diagnosis Test ===")

        val queueName = "recovery-test-${System.currentTimeMillis()}"
        val tenantId = "recovery-test"

        // 큐 생성
        queueService.createQueue(
            tenantId, queueName,
            QueueAttributes(visibilityTimeoutSeconds = 30, messageRetentionPeriod = 1209600)
        ).block()

        delay(1000) // 큐 생성 대기

        // 테스트 파라미터
        val totalMessages = 100 // 적은 수로 시작해서 정확한 분석
        val messagePayload = "test_message_payload"

        logger.info("📤 Sending $totalMessages messages...")

        // 메시지 전송
        val sendTime = measureTimeMillis {
            repeat(totalMessages) { index ->
                messageService.sendMessage(
                    tenantId = tenantId,
                    queueName = queueName,
                    request = SendMessageRequest(
                        messageBody = "$messagePayload-$index",
                        delaySeconds = 0,
                        attributes = mapOf("index" to index.toString())
                    )
                ).block()
            }
        }

        logger.info("✅ Sent $totalMessages messages in ${sendTime}ms")

        // 전송 후 대기 시간 늘리기
        logger.info("⏳ Waiting for message propagation...")
        delay(3000) // 3초 대기

        // 메시지 수신 - 더 꼼꼼하게
        logger.info("📥 Receiving messages...")
        val receivedMessages = mutableListOf<String>()
        var consecutiveEmpty = 0
        val maxConsecutiveEmpty = 10 // 더 많은 시도

        val receiveTime = measureTimeMillis {
            while (consecutiveEmpty < maxConsecutiveEmpty) {
                try {
                    val messages = messageService.receiveMessages(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = ReceiveMessageRequest(
                            maxNumberOfMessages = 10,
                            waitTimeSeconds = 2, // 더 긴 대기
                            visibilityTimeoutSeconds = 30
                        )
                    ).collectList().block() ?: emptyList()

                    if (messages.isNotEmpty()) {
                        logger.info("📨 Received ${messages.size} messages")
                        receivedMessages.addAll(messages.map { it.messageBody })
                        consecutiveEmpty = 0
                    } else {
                        consecutiveEmpty++
                        logger.info("⏸️ Empty poll #$consecutiveEmpty")
                        delay(100L) // 짧은 대기
                    }
                } catch (e: Exception) {
                    logger.warn("❌ Receive error: ${e.message}")
                    consecutiveEmpty++
                    delay(200L)
                }
            }
        }

        val recoveryRate = if (totalMessages > 0) receivedMessages.size * 100.0 / totalMessages else 0.0

        logger.info("")
        logger.info("📊 Recovery Rate Test Results:")
        logger.info("- Messages sent: $totalMessages")
        logger.info("- Messages received: ${receivedMessages.size}")
        logger.info("- Recovery rate: ${String.format("%.1f", recoveryRate)}%")
        logger.info("- Send time: ${sendTime}ms")
        logger.info("- Receive time: ${receiveTime}ms")

        // 상세 분석
        analyzeRecoveryIssues(totalMessages, receivedMessages.size, recoveryRate)
    }

    @Test
    fun `improved recovery rate test`() = runBlocking {
        logger.info("=== Improved Recovery Rate Test ===")

        val queueName = "improved-recovery-${System.currentTimeMillis()}"
        val tenantId = "improved-test"

        // 큐 생성 (더 긴 visibility timeout)
        queueService.createQueue(
            tenantId, queueName,
            QueueAttributes(
                visibilityTimeoutSeconds = 60, // 더 긴 visibility timeout
                messageRetentionPeriod = 1209600
            )
        ).block()

        delay(2000) // 충분한 큐 생성 대기

        val totalMessages = 100
        val messagePayload = "improved_test_message"

        logger.info("📤 Sending $totalMessages messages with improved settings...")

        // 개선된 메시지 전송 (더 안정적으로)
        val sendTime = measureTimeMillis {
            repeat(totalMessages) { index ->
                try {
                    messageService.sendMessage(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = SendMessageRequest(
                            messageBody = "$messagePayload-$index-${System.currentTimeMillis()}",
                            delaySeconds = 0,
                            attributes = mapOf(
                                "index" to index.toString(),
                                "timestamp" to System.currentTimeMillis().toString()
                            )
                        )
                    ).block()

                    // 전송 간 짧은 대기 (백프레셔 방지)
                    if (index % 10 == 9) {
                        delay(10L)
                    }
                } catch (e: Exception) {
                    logger.warn("❌ Send error for message $index: ${e.message}")
                }
            }
        }

        logger.info("✅ Sent $totalMessages messages in ${sendTime}ms")

        // 더 긴 전파 대기
        logger.info("⏳ Waiting for message propagation (5 seconds)...")
        delay(5000)

        // 개선된 메시지 수신
        logger.info("📥 Receiving messages with improved polling...")
        val receivedMessages = mutableSetOf<String>() // Set으로 중복 방지
        var totalPolls = 0
        var emptyPolls = 0
        val maxEmptyPolls = 15 // 더 많은 재시도

        val receiveTime = measureTimeMillis {
            while (emptyPolls < maxEmptyPolls && receivedMessages.size < totalMessages) {
                totalPolls++
                try {
                    val messages = messageService.receiveMessages(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = ReceiveMessageRequest(
                            maxNumberOfMessages = 10,
                            waitTimeSeconds = 3, // 더 긴 long polling
                            visibilityTimeoutSeconds = 60
                        )
                    ).collectList().block() ?: emptyList()

                    if (messages.isNotEmpty()) {
                        val newMessages = messages.map { it.messageBody }.filter { !receivedMessages.contains(it) }
                        receivedMessages.addAll(newMessages)
                        emptyPolls = 0
                        logger.info("📨 Poll #$totalPolls: Received ${messages.size} messages (${newMessages.size} new), Total: ${receivedMessages.size}")
                    } else {
                        emptyPolls++
                        logger.info("⏸️ Poll #$totalPolls: Empty (consecutive: $emptyPolls)")
                    }

                    // 동적 백오프
                    val backoffDelay = minOf(50L + (emptyPolls * 25L), 500L)
                    delay(backoffDelay)

                } catch (e: Exception) {
                    logger.warn("❌ Receive error on poll #$totalPolls: ${e.message}")
                    emptyPolls++
                    delay(300L)
                }
            }
        }

        val improvedRecoveryRate = if (totalMessages > 0) receivedMessages.size * 100.0 / totalMessages else 0.0

        logger.info("")
        logger.info("📊 Improved Recovery Rate Test Results:")
        logger.info("- Messages sent: $totalMessages")
        logger.info("- Messages received: ${receivedMessages.size}")
        logger.info("- Recovery rate: ${String.format("%.1f", improvedRecoveryRate)}%")
        logger.info("- Total polls: $totalPolls")
        logger.info("- Empty polls: $emptyPolls")
        logger.info("- Send time: ${sendTime}ms")
        logger.info("- Receive time: ${receiveTime}ms")

        // 개선 효과 분석
        analyzeImprovements(improvedRecoveryRate)
    }

    private fun analyzeRecoveryIssues(sent: Int, received: Int, recoveryRate: Double) {
        logger.info("")
        logger.info("🔍 Recovery Rate Analysis:")

        when {
            recoveryRate >= 95.0 -> {
                logger.info("🟢 EXCELLENT: Recovery rate is very high")
                logger.info("- System is working optimally")
            }

            recoveryRate >= 85.0 -> {
                logger.info("🟡 GOOD: Recovery rate is acceptable")
                logger.info("- Minor message loss, within acceptable range")
            }

            recoveryRate >= 70.0 -> {
                logger.info("🟠 MODERATE: Some messages are being lost")
                logger.info("- Possible causes:")
                logger.info("  * Insufficient polling time")
                logger.info("  * Message visibility timeout too short")
                logger.info("  * Race conditions in concurrent access")
            }

            else -> {
                logger.info("🔴 POOR: Significant message loss detected")
                logger.info("- Likely causes:")
                logger.info("  * Messages not properly persisted")
                logger.info("  * Polling strategy inadequate")
                logger.info("  * Storage/persistence issues")
                logger.info("  * Timing issues in test setup")
            }
        }

        logger.info("")
        logger.info("💡 Improvement Suggestions:")
        logger.info("1. Increase message propagation delay (current: 3s → try 5s)")
        logger.info("2. Increase visibility timeout (current: 30s → try 60s)")
        logger.info("3. Use longer long polling (current: 2s → try 5s)")
        logger.info("4. Increase max polling attempts")
        logger.info("5. Add exponential backoff between polls")
        logger.info("6. Check for duplicate message handling")
    }

    @Test
    fun `optimized recovery rate test with enhanced strategies`() = runBlocking {
        logger.info("=== Optimized Recovery Rate Test ===")

        val queueName = "optimized-recovery-${System.currentTimeMillis()}"
        val tenantId = "optimized-test"

        // 큐 생성 (개선된 설정)
        queueService.createQueue(
            tenantId, queueName,
            QueueAttributes(
                visibilityTimeoutSeconds = 120, // 더 긴 visibility timeout (30s → 120s)
                messageRetentionPeriod = 1209600
            )
        ).block()

        delay(3000) // 충분한 큐 생성 대기

        val totalMessages = 100
        val messagePayload = "optimized_test_message"

        logger.info("📤 Sending $totalMessages messages with optimized settings...")

        // 병렬 메시지 전송으로 처리량 향상
        val sendTime = measureTimeMillis {
            val sendJobs = (0 until totalMessages).chunked(10).map { chunk ->
                async {
                    chunk.forEach { index ->
                        try {
                            messageService.sendMessage(
                                tenantId = tenantId,
                                queueName = queueName,
                                request = SendMessageRequest(
                                    messageBody = "$messagePayload-$index-${System.currentTimeMillis()}",
                                    delaySeconds = 0,
                                    attributes = mapOf(
                                        "index" to index.toString(),
                                        "timestamp" to System.currentTimeMillis().toString(),
                                        "batch" to (index / 10).toString()
                                    )
                                )
                            ).block()

                            // 전송 간 짧은 대기 (백프레셔 방지)
                            if (index % 10 == 9) {
                                delay(10L)
                            }
                        } catch (e: Exception) {
                            logger.warn("❌ Send error for message $index: ${e.message}")
                        }
                    }
                }
            }
            sendJobs.awaitAll()
        }

        logger.info("✅ Sent $totalMessages messages in ${sendTime}ms")

        // 더 긴 전파 대기 (3s → 8s)
        logger.info("⏳ Waiting for message propagation (8 seconds)...")
        delay(8000)

        // 개선된 메시지 수신 - 더 적극적인 폴링
        logger.info("📥 Receiving messages with enhanced polling strategies...")
        val receivedMessages = mutableSetOf<String>() // Set으로 중복 방지
        var totalPolls = 0
        var emptyPolls = 0
        val maxEmptyPolls = 20 // 더 많은 재시도 (10 → 20)

        val receiveTime = measureTimeMillis {
            while (emptyPolls < maxEmptyPolls && receivedMessages.size < totalMessages) {
                totalPolls++
                try {
                    // 동적 배치 크기: 초기에는 더 많이, 나중에는 적게 (최대 10으로 제한)
                    val batchSize = when {
                        receivedMessages.size < totalMessages * 0.7 -> 10 // 70% 미만일 때는 최대
                        receivedMessages.size < totalMessages * 0.9 -> 8 // 90% 미만일 때는 보통
                        else -> 5 // 90% 이상일 때는 작게
                    }

                    val messages = messageService.receiveMessages(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = ReceiveMessageRequest(
                            maxNumberOfMessages = batchSize,
                            waitTimeSeconds = 5, // 더 긴 long polling (2s → 5s)
                            visibilityTimeoutSeconds = 120
                        )
                    ).collectList().block() ?: emptyList()

                    if (messages.isNotEmpty()) {
                        val newMessages = messages.map { it.messageBody }.filter { !receivedMessages.contains(it) }
                        receivedMessages.addAll(newMessages)
                        emptyPolls = 0
                        logger.info("📨 Poll #$totalPolls: Received ${messages.size} messages (${newMessages.size} new), Total: ${receivedMessages.size}")
                    } else {
                        emptyPolls++
                        logger.info("⏸️ Poll #$totalPolls: Empty (consecutive: $emptyPolls)")
                    }

                    // 지수적 백오프 (더 스마트한 대기)
                    val backoffDelay = when {
                        emptyPolls <= 3 -> 25L // 처음 3번은 짧게
                        emptyPolls <= 7 -> 100L // 4-7번은 보통
                        emptyPolls <= 12 -> 250L // 8-12번은 길게
                        else -> 500L // 그 이후는 더 길게
                    }
                    delay(backoffDelay)

                } catch (e: Exception) {
                    logger.warn("❌ Receive error on poll #$totalPolls: ${e.message}")
                    emptyPolls++
                    delay(500L)
                }
            }
        }

        val optimizedRecoveryRate = if (totalMessages > 0) receivedMessages.size * 100.0 / totalMessages else 0.0

        logger.info("")
        logger.info("📊 Optimized Recovery Rate Test Results:")
        logger.info("- Messages sent: $totalMessages")
        logger.info("- Messages received: ${receivedMessages.size}")
        logger.info("- Recovery rate: ${String.format("%.1f", optimizedRecoveryRate)}%")
        logger.info("- Total polls: $totalPolls")
        logger.info("- Empty polls: $emptyPolls")
        logger.info("- Send time: ${sendTime}ms")
        logger.info("- Receive time: ${receiveTime}ms")

        // 개선 효과 분석
        analyzeOptimizedResults(optimizedRecoveryRate, totalPolls, emptyPolls)
    }

    private fun analyzeImprovements(improvedRate: Double) {
        logger.info("")
        logger.info("📈 Improvement Analysis:")

        val improvements = mutableListOf<String>()

        if (improvedRate >= 95.0) {
            improvements.add("✅ Excellent recovery rate achieved")
        }
        if (improvedRate >= 90.0) {
            improvements.add("✅ Long polling strategy effective")
        }
        if (improvedRate >= 80.0) {
            improvements.add("✅ Extended timeouts helping")
        }

        improvements.forEach { logger.info(it) }

        if (improvedRate < 90.0) {
            logger.info("")
            logger.info("🔧 Further improvements needed:")
            logger.info("- Consider persistent storage issues")
            logger.info("- Check queue implementation for race conditions")
            logger.info("- Verify message ordering and deduplication")
        }
    }

    private fun analyzeOptimizedResults(recoveryRate: Double, totalPolls: Int, emptyPolls: Int) {
        logger.info("")
        logger.info("🚀 Optimized Recovery Analysis:")

        when {
            recoveryRate >= 98.0 -> {
                logger.info("🟢 EXCELLENT: Near-perfect recovery achieved!")
                logger.info("- Optimization strategies highly effective")
                logger.info("- Message persistence and retrieval working optimally")
                logger.info("- Partition-based architecture performing well")
            }

            recoveryRate >= 95.0 -> {
                logger.info("🟢 EXCELLENT: Very high recovery rate")
                logger.info("- Extended timeouts and long polling effective")
                logger.info("- Dynamic batch sizing working well")
            }

            recoveryRate >= 90.0 -> {
                logger.info("✅ GOOD: Recovery rate significantly improved")
                logger.info("- Most messages successfully retrieved")
                logger.info("- Minor optimization opportunities remain")
            }

            recoveryRate >= 85.0 -> {
                logger.info("🟡 MODERATE: Improvement achieved but more work needed")
                logger.info("- Partial success with optimization strategies")
                logger.info("- May need architectural improvements")
            }

            else -> {
                logger.info("❌ POOR: Optimization strategies need refinement")
                logger.info("- Core issues may exist in message persistence layer")
                logger.info("- Consider architectural review")
            }
        }

        logger.info("")
        logger.info("📈 Performance Metrics:")
        logger.info("- Polling efficiency: ${String.format("%.1f", (totalPolls - emptyPolls) * 100.0 / totalPolls)}%")
        logger.info("- Avg messages per successful poll: ${String.format("%.1f", recoveryRate / (totalPolls - emptyPolls))}")

        if (recoveryRate >= 95.0) {
            logger.info("")
            logger.info("🎯 Optimization Success Factors:")
            logger.info("✅ Extended visibility timeout (120s)")
            logger.info("✅ Longer long polling (5s)")
            logger.info("✅ Dynamic batch sizing")
            logger.info("✅ Enhanced propagation delay (8s)")
            logger.info("✅ Exponential backoff strategy")
            logger.info("✅ Parallel message sending")
        }
    }
}
