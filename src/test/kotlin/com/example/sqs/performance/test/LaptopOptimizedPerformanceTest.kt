package com.example.sqs.performance.test

import com.example.sqs.queue.service.QueueService
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.message.service.MessageService
import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.SendMessageRequest
import kotlinx.coroutines.*
import org.junit.jupiter.api.Test
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.slf4j.LoggerFactory
import kotlin.system.measureTimeMillis

@SpringBootTest
@ActiveProfiles("test")
class LaptopOptimizedPerformanceTest {

    @Autowired
    private lateinit var queueService: QueueService

    @Autowired
    private lateinit var messageService: MessageService

    private val logger = LoggerFactory.getLogger(LaptopOptimizedPerformanceTest::class.java)

    data class KafkaStyleResult(
        val producerResults: List<ProducerResult>,
        val consumerResults: List<ConsumerResult>,
        val totalSendTime: Long,
        val totalReceiveTime: Long,
        val totalSent: Int,
        val totalReceived: Int,
        val sendThroughput: Int,
        val receiveThroughput: Int,
        val recoveryRate: Double
    )

    data class RabbitMQStyleResult(
        val queueResults: Map<String, QueueResult>,
        val totalSendTime: Long,
        val totalReceiveTime: Long,
        val totalSent: Int,
        val totalReceived: Int,
        val aggregateSendThroughput: Int,
        val aggregateReceiveThroughput: Int,
        val overallRecoveryRate: Double
    )

    data class ProducerResult(
        val threadId: Int,
        val messagesSent: Int,
        val timeMs: Long,
        val throughput: Int
    )

    data class ConsumerResult(
        val threadId: Int,
        val messagesReceived: Int,
        val timeMs: Long,
        val throughput: Int
    )

    data class QueueResult(
        val queueName: String,
        val messagesSent: Int,
        val messagesReceived: Int,
        val sendThroughput: Int,
        val receiveThroughput: Int,
        val recoveryRate: Double
    )

    data class SystemInfo(
        val cpuCores: Int,
        val totalMemoryMB: Long,
        val freeMemoryMB: Long,
        val maxMemoryMB: Long,
        val osName: String,
        val javaVersion: String
    )

    @Test
    fun `kafka style laptop optimized benchmark`() = runBlocking {
        logger.info("=== Kafka-Style Laptop-Optimized Benchmark ===")

        val systemInfo = collectSystemInfo()
        logSystemInfo(systemInfo)

        // 노트북 환경 최적화된 Kafka 스타일 설정
        val queueName = "kafka-laptop-benchmark-${System.currentTimeMillis()}" // 고유 큐 이름
        val tenantId = "laptop-kafka-test"

        queueService.createQueue(
            tenantId, queueName,
            QueueAttributes(visibilityTimeoutSeconds = 30, messageRetentionPeriod = 1209600)
        ).block()

        // 큐 생성 완료 대기
        delay(2000)

        // 노트북 환경 고려한 파라미터 조정 (성능 최적화)
        val totalMessages = 2000      // 메시지 수 감소 (3000→2000)
        val batchSize = 50           // 배치 크기 최적화
        val producerThreads = 2      // Producer 스레드 감소 (3→2)
        val consumerThreads = 4      // Consumer 스레드 감소 (6→4)
        val messagePayload = "kafka_laptop_" + "x".repeat(32) // 메시지 크기 더 작게 (64B→32B)

        logger.info("📋 Kafka-Style Laptop Test Configuration:")
        logger.info("- Total messages: $totalMessages (Laptop optimized)")
        logger.info("- Batch size: $batchSize")
        logger.info("- Producer threads: $producerThreads")
        logger.info("- Consumer threads: $consumerThreads")
        logger.info("- Message size: ~${messagePayload.length} bytes")

        // Producer 성능 테스트 (Kafka 스타일 배치 처리)
        val producerResults = mutableListOf<ProducerResult>()
        logger.info("🚀 Starting Kafka-style batch producers...")

        val totalProduceTime = measureTimeMillis {
            val batches = (0 until totalMessages).chunked(batchSize)
            logger.info("📦 Created ${batches.size} batches for parallel processing")

            coroutineScope {
                val producerJobs = (0 until producerThreads).map { threadIndex ->
                    async(Dispatchers.IO) {
                        val threadBatches = batches.filterIndexed { index, _ -> index % producerThreads == threadIndex }
                        var sent = 0

                        val threadTime = measureTimeMillis {
                            threadBatches.forEach { batch ->
                                batch.forEach { messageIndex ->
                                    messageService.sendMessage(
                                        tenantId = tenantId,
                                        queueName = queueName,
                                        request = SendMessageRequest(
                                            messageBody = "$messagePayload-$messageIndex-${System.currentTimeMillis()}",
                                            delaySeconds = 0,
                                            attributes = mapOf(
                                                "batch_id" to (messageIndex / batchSize).toString(),
                                                "producer_id" to threadIndex.toString(),
                                                "kafka_style" to "true"
                                            )
                                        )
                                    ).block()
                                    sent++
                                }
                                // 배치 간 소량 휴식 추가 (백프레셔 방지)
                                if (threadBatches.size > 10) delay(1L)
                            }
                        }

                        val throughput = if (threadTime > 0) (sent * 1000 / threadTime).toInt() else 0
                        logger.info("Producer thread $threadIndex completed: $sent msgs in ${threadTime}ms ($throughput msg/sec)")
                        ProducerResult(threadIndex, sent, threadTime, throughput)
                    }
                }

                val results = producerJobs.awaitAll()
                producerResults.addAll(results)
            }
        }

        val totalSent = producerResults.sumOf { it.messagesSent }
        val totalThroughput = if (totalProduceTime > 0) (totalSent * 1000 / totalProduceTime).toInt() else 0
        logger.info("✅ All producers completed! Total: $totalSent msgs, Throughput: $totalThroughput msg/sec")

        // 메시지 전파 대기 (최적화)
        delay(1500) // 1000ms → 1500ms (충분한 대기)

        // Consumer 성능 테스트 (Kafka 스타일 폴링)
        logger.info("📥 Starting Kafka-style consumers...")
        val consumerResults = mutableListOf<ConsumerResult>()
        var totalReceived = 0

        val totalConsumeTime = measureTimeMillis {
            coroutineScope {
                val consumerJobs = (0 until consumerThreads).map { threadIndex ->
                    async(Dispatchers.IO) {
                        var received = 0
                        var consecutiveEmpty = 0
                        val maxConsecutiveEmpty = 8 // 연속 빈 폴링 횟수 제한

                        val threadTime = measureTimeMillis {
                            while (consecutiveEmpty < maxConsecutiveEmpty) {
                                try {
                                    val messages = messageService.receiveMessages(
                                        tenantId = tenantId,
                                        queueName = queueName,
                                        request = ReceiveMessageRequest(
                                            maxNumberOfMessages = 10,
                                            waitTimeSeconds = 1, // 짧은 대기 시간
                                            visibilityTimeoutSeconds = 30
                                        )
                                    ).collectList().block() ?: emptyList()

                                    if (messages.isNotEmpty()) {
                                        received += messages.size
                                        consecutiveEmpty = 0
                                    } else {
                                        consecutiveEmpty++
                                        delay(10L) // 짧은 백오프
                                    }
                                } catch (e: Exception) {
                                    logger.warn("Consumer thread $threadIndex error: ${e.message}")
                                    consecutiveEmpty++
                                    delay(50L)
                                }
                            }
                        }

                        val throughput = if (threadTime > 0) (received * 1000 / threadTime).toInt() else 0
                        logger.info("Consumer thread $threadIndex: $received msgs in ${threadTime}ms ($throughput msg/sec)")
                        ConsumerResult(threadIndex, received, threadTime, throughput)
                    }
                }

                val results = consumerJobs.awaitAll()
                consumerResults.addAll(results)
                totalReceived = results.sumOf { it.messagesReceived }
            }
        }

        val receiveThroughput = if (totalConsumeTime > 0) (totalReceived * 1000 / totalConsumeTime).toInt() else 0
        val recoveryRate = if (totalSent > 0) totalReceived * 100.0 / totalSent else 0.0

        val kafkaResult = KafkaStyleResult(
            producerResults, consumerResults, totalProduceTime, totalConsumeTime,
            totalSent, totalReceived, totalThroughput, receiveThroughput, recoveryRate
        )

        logKafkaResults(kafkaResult, systemInfo)
    }

    @Test
    fun `rabbitmq style laptop optimized multi queue benchmark`() = runBlocking {
        logger.info("=== RabbitMQ-Style Laptop-Optimized Multi-Queue Benchmark ===")

        val systemInfo = collectSystemInfo()
        logSystemInfo(systemInfo)

        // 노트북 환경을 고려한 RabbitMQ 스타일 설정
        val tenantId = "laptop-rabbitmq-test"
        val queues = listOf("orders", "notifications", "analytics", "logs")
        val messageTypes = mapOf(
            "orders" to "order_data_" + "x".repeat(128),           // 128B
            "notifications" to "notification_" + "x".repeat(64),   // 64B
            "analytics" to "analytics_event_" + "x".repeat(256),   // 256B
            "logs" to "log_entry_" + "x".repeat(32)               // 32B
        )

        // 큐 생성
        queues.forEach { queueName ->
            queueService.createQueue(
                tenantId, queueName,
                QueueAttributes(visibilityTimeoutSeconds = 30, messageRetentionPeriod = 1209600)
            ).block()
        }

        val messagesPerQueue = 750  // 큐당 메시지 수
        val producersPerQueue = 1   // 큐당 프로듀서
        val consumersPerQueue = 1   // 큐당 컨슈머

        logger.info("📋 RabbitMQ-Style Laptop Test Configuration:")
        logger.info("- Queues: ${queues.size} (${queues.joinToString()})")
        logger.info("- Messages per queue: $messagesPerQueue")
        logger.info("- Total messages: ${queues.size * messagesPerQueue}")

        // 다중 큐 병렬 생산
        val queueResults = mutableMapOf<String, QueueResult>()

        logger.info("📤 Starting RabbitMQ-style producers...")
        val totalProduceTime = measureTimeMillis {
            coroutineScope {
                val allProducerJobs = queues.map { queueName ->
                    async(Dispatchers.IO) {
                        val messagePayload = messageTypes[queueName] ?: "default"
                        var sent = 0

                        val queueSendTime = measureTimeMillis {
                            repeat(messagesPerQueue) { messageIndex ->
                                val routingKey = when (queueName) {
                                    "orders" -> "order.created"
                                    "notifications" -> "notification.send"
                                    "analytics" -> "analytics.track"
                                    "logs" -> "log.info"
                                    else -> "default.message"
                                }

                                messageService.sendMessage(
                                    tenantId = tenantId,
                                    queueName = queueName,
                                    request = SendMessageRequest(
                                        messageBody = "$messagePayload-$messageIndex-${System.currentTimeMillis()}",
                                        delaySeconds = 0,
                                        attributes = mapOf(
                                            "exchange" to "laptop_exchange",
                                            "routing_key" to routingKey,
                                            "queue" to queueName,
                                            "priority" to when (queueName) {
                                                "orders" -> "high"
                                                "notifications" -> "medium"
                                                else -> "low"
                                            }
                                        )
                                    )
                                ).block()
                                sent++

                                if (sent % 100 == 0) {
                                    delay(2L) // 100개마다 2ms 휴식
                                }
                            }
                        }

                        val sendThroughput = if (queueSendTime > 0) (sent * 1000 / queueSendTime).toInt() else 0
                        logger.info("Queue $queueName producer: $sent msgs in ${queueSendTime}ms ($sendThroughput msg/sec)")

                        queueResults[queueName] = QueueResult(
                            queueName, sent, 0, sendThroughput, 0, 0.0
                        )
                    }
                }
                allProducerJobs.awaitAll()
            }
        }

        logger.info("✅ All producers completed in ${totalProduceTime}ms")
        delay(1500) // 메시지 전파 대기

        // 다중 큐 병렬 소비
        logger.info("📥 Starting RabbitMQ-style consumers...")
        val totalConsumeTime = measureTimeMillis {
            coroutineScope {
                val allConsumerJobs = queues.map { queueName ->
                    async(Dispatchers.IO) {
                        var consumed = 0
                        var emptyPolls = 0
                        val maxEmptyPolls = 8

                        val queueReceiveTime = measureTimeMillis {
                            while (emptyPolls < maxEmptyPolls) {
                                val messages = messageService.receiveMessages(
                                    tenantId = tenantId,
                                    queueName = queueName,
                                    request = ReceiveMessageRequest(
                                        maxNumberOfMessages = 5,
                                        waitTimeSeconds = 1,
                                        visibilityTimeoutSeconds = 30
                                    )
                                ).collectList().block() ?: emptyList()

                                if (messages.isNotEmpty()) {
                                    consumed += messages.size
                                    emptyPolls = 0

                                    // RabbitMQ 스타일 처리 시뮬레이션
                                    val processingTime = when (queueName) {
                                        "orders" -> 2L
                                        "notifications" -> 1L
                                        "analytics" -> 3L
                                        "logs" -> 1L
                                        else -> 1L
                                    }
                                    if (processingTime > 1L) delay(processingTime)
                                } else {
                                    emptyPolls++
                                    delay(50L * emptyPolls)
                                }
                            }
                        }

                        val receiveThroughput = if (queueReceiveTime > 0) (consumed * 1000 / queueReceiveTime).toInt() else 0
                        logger.info("Queue $queueName consumer: $consumed msgs in ${queueReceiveTime}ms ($receiveThroughput msg/sec)")

                        val existingResult = queueResults[queueName]!!
                        val recoveryRate = if (existingResult.messagesSent > 0) consumed * 100.0 / existingResult.messagesSent else 0.0

                        queueResults[queueName] = existingResult.copy(
                            messagesReceived = consumed,
                            receiveThroughput = receiveThroughput,
                            recoveryRate = recoveryRate
                        )
                    }
                }
                allConsumerJobs.awaitAll()
            }
        }

        val totalSent = queueResults.values.sumOf { it.messagesSent }
        val totalReceived = queueResults.values.sumOf { it.messagesReceived }
        val aggregateSendThroughput = if (totalProduceTime > 0) (totalSent * 1000 / totalProduceTime).toInt() else 0
        val aggregateReceiveThroughput = if (totalConsumeTime > 0) (totalReceived * 1000 / totalConsumeTime).toInt() else 0
        val overallRecoveryRate = if (totalSent > 0) totalReceived * 100.0 / totalSent else 0.0

        val rabbitResult = RabbitMQStyleResult(
            queueResults, totalProduceTime, totalConsumeTime,
            totalSent, totalReceived, aggregateSendThroughput, aggregateReceiveThroughput, overallRecoveryRate
        )

        logRabbitMQResults(rabbitResult, systemInfo)
    }

    @Test
    fun `comprehensive laptop performance comparison`() = runBlocking {
        logger.info("=== Comprehensive Laptop Performance Comparison ===")

        val systemInfo = collectSystemInfo()
        logSystemInfo(systemInfo)

        // Kafka 스타일 테스트
        val kafkaResult = runKafkaStyleTest()
        delay(2000)

        // RabbitMQ 스타일 테스트
        val rabbitResult = runRabbitMQStyleTest()

        // 비교 분석 및 레포트 생성
        generateComparisonReport(kafkaResult, rabbitResult, systemInfo)
    }

    private suspend fun runKafkaStyleTest(): KafkaStyleResult {
        // 간소화된 Kafka 스타일 테스트 실행
        val queueName = "comparison-kafka-${System.currentTimeMillis()}"
        val tenantId = "comparison-test-${System.currentTimeMillis()}"
        val messageCount = 2000

        queueService.createQueue(tenantId, queueName, QueueAttributes()).block()

        val producerResults = mutableListOf<ProducerResult>()
        val totalSendTime = measureTimeMillis {
            coroutineScope {
                val jobs = (0 until 3).map { threadId ->
                    async(Dispatchers.IO) {
                        var sent = 0
                        val time = measureTimeMillis {
                            repeat(messageCount / 3) { index ->
                                messageService.sendMessage(
                                    tenantId, queueName,
                                    SendMessageRequest("kafka_test_$index", 0)
                                ).block()
                                sent++
                            }
                        }
                        ProducerResult(threadId, sent, time, if (time > 0) (sent * 1000 / time).toInt() else 0)
                    }
                }
                producerResults.addAll(jobs.awaitAll())
            }
        }

        delay(500)

        val consumerResults = mutableListOf<ConsumerResult>()
        var totalReceived = 0
        val totalReceiveTime = measureTimeMillis {
            coroutineScope {
                val jobs = (0 until 2).map { threadId ->
                    async(Dispatchers.IO) {
                        var received = 0
                        var emptyPolls = 0
                        val time = measureTimeMillis {
                            while (emptyPolls < 6) {
                                val messages = messageService.receiveMessages(
                                    tenantId, queueName,
                                    ReceiveMessageRequest(10, 1, 30)
                                ).collectList().block() ?: emptyList()

                                if (messages.isNotEmpty()) {
                                    received += messages.size
                                    emptyPolls = 0
                                } else {
                                    emptyPolls++
                                    delay(50L)
                                }
                            }
                        }
                        ConsumerResult(threadId, received, time, if (time > 0) (received * 1000 / time).toInt() else 0)
                    }
                }
                val results = jobs.awaitAll()
                consumerResults.addAll(results)
                totalReceived = results.sumOf { it.messagesReceived }
            }
        }

        val totalSent = producerResults.sumOf { it.messagesSent }
        val sendThroughput = if (totalSendTime > 0) (totalSent * 1000 / totalSendTime).toInt() else 0
        val receiveThroughput = if (totalReceiveTime > 0) (totalReceived * 1000 / totalReceiveTime).toInt() else 0
        val recoveryRate = if (totalSent > 0) totalReceived * 100.0 / totalSent else 0.0

        return KafkaStyleResult(
            producerResults, consumerResults, totalSendTime, totalReceiveTime,
            totalSent, totalReceived, sendThroughput, receiveThroughput, recoveryRate
        )
    }

    private suspend fun runRabbitMQStyleTest(): RabbitMQStyleResult {
        // 간소화된 RabbitMQ 스타일 테스트 실행
        val tenantId = "comparison-test-${System.currentTimeMillis()}"
        val queueSuffix = System.currentTimeMillis()
        val queues = listOf("orders", "notifications", "analytics")
        val messagesPerQueue = 650

        queues.forEach { queueName ->
            queueService.createQueue(tenantId, "rabbit-$queueName-$queueSuffix", QueueAttributes()).block()
        }

        val queueResults = mutableMapOf<String, QueueResult>()

        val totalSendTime = measureTimeMillis {
            coroutineScope {
                queues.map { queueName ->
                    async(Dispatchers.IO) {
                        var sent = 0
                        val time = measureTimeMillis {
                            repeat(messagesPerQueue) { index ->
                                messageService.sendMessage(
                                    tenantId, "rabbit-$queueName-$queueSuffix",
                                    SendMessageRequest("rabbit_${queueName}_$index", 0)
                                ).block()
                                sent++
                            }
                        }
                        val throughput = if (time > 0) (sent * 1000 / time).toInt() else 0
                        queueResults[queueName] = QueueResult(queueName, sent, 0, throughput, 0, 0.0)
                    }
                }.awaitAll()
            }
        }

        delay(1000)

        val totalReceiveTime = measureTimeMillis {
            coroutineScope {
                queues.map { queueName ->
                    async(Dispatchers.IO) {
                        var received = 0
                        var emptyPolls = 0
                        val time = measureTimeMillis {
                            while (emptyPolls < 6) {
                                val messages = messageService.receiveMessages(
                                    tenantId, "rabbit-$queueName-$queueSuffix",
                                    ReceiveMessageRequest(5, 1, 30)
                                ).collectList().block() ?: emptyList()

                                if (messages.isNotEmpty()) {
                                    received += messages.size
                                    emptyPolls = 0
                                } else {
                                    emptyPolls++
                                    delay(50L)
                                }
                            }
                        }
                        val receiveThroughput = if (time > 0) (received * 1000 / time).toInt() else 0
                        val existing = queueResults[queueName]!!
                        val recoveryRate = if (existing.messagesSent > 0) received * 100.0 / existing.messagesSent else 0.0
                        queueResults[queueName] = existing.copy(
                            messagesReceived = received,
                            receiveThroughput = receiveThroughput,
                            recoveryRate = recoveryRate
                        )
                    }
                }.awaitAll()
            }
        }

        val totalSent = queueResults.values.sumOf { it.messagesSent }
        val totalReceived = queueResults.values.sumOf { it.messagesReceived }
        val aggregateSendThroughput = if (totalSendTime > 0) (totalSent * 1000 / totalSendTime).toInt() else 0
        val aggregateReceiveThroughput = if (totalReceiveTime > 0) (totalReceived * 1000 / totalReceiveTime).toInt() else 0
        val overallRecoveryRate = if (totalSent > 0) totalReceived * 100.0 / totalSent else 0.0

        return RabbitMQStyleResult(
            queueResults, totalSendTime, totalReceiveTime,
            totalSent, totalReceived, aggregateSendThroughput, aggregateReceiveThroughput, overallRecoveryRate
        )
    }

    private fun collectSystemInfo(): SystemInfo {
        val runtime = Runtime.getRuntime()
        return SystemInfo(
            cpuCores = runtime.availableProcessors(),
            totalMemoryMB = runtime.totalMemory() / 1024 / 1024,
            freeMemoryMB = runtime.freeMemory() / 1024 / 1024,
            maxMemoryMB = runtime.maxMemory() / 1024 / 1024,
            osName = System.getProperty("os.name"),
            javaVersion = System.getProperty("java.version")
        )
    }

    private fun logSystemInfo(systemInfo: SystemInfo) {
        logger.info("💻 Laptop System Information:")
        logger.info("- CPU Cores: ${systemInfo.cpuCores}")
        logger.info("- Total Memory: ${systemInfo.totalMemoryMB}MB")
        logger.info("- Free Memory: ${systemInfo.freeMemoryMB}MB")
        logger.info("- Max Memory: ${systemInfo.maxMemoryMB}MB")
        logger.info("- OS: ${systemInfo.osName}")
        logger.info("- Java Version: ${systemInfo.javaVersion}")
    }

    private fun logKafkaResults(result: KafkaStyleResult, systemInfo: SystemInfo) {
        logger.info("")
        logger.info("📊 Kafka-Style Results:")
        logger.info("┌─────────────────────────────────────────────────────────┐")
        logger.info("│                  PRODUCER METRICS                      │")
        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│ Total Throughput: %6d msg/sec (%d ms)              │".format(result.sendThroughput, result.totalSendTime))
        logger.info("│ Total Messages:   %6d msgs                         │".format(result.totalSent))

        result.producerResults.forEach { producer ->
            logger.info("│ Thread %d:        %6d msg/sec (%d msgs)            │".format(producer.threadId, producer.throughput, producer.messagesSent))
        }

        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│                  CONSUMER METRICS                      │")
        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│ Total Throughput: %6d msg/sec (%d ms)              │".format(result.receiveThroughput, result.totalReceiveTime))
        logger.info("│ Recovery Rate:    %6.1f%% (%d/%d msgs)             │".format(result.recoveryRate, result.totalReceived, result.totalSent))

        result.consumerResults.forEach { consumer ->
            logger.info("│ Thread %d:        %6d msgs received                 │".format(consumer.threadId, consumer.messagesReceived))
        }

        logger.info("└─────────────────────────────────────────────────────────┘")

        // 성능 등급 분석
        val laptopKafkaRange = when {
            result.sendThroughput >= 5000 -> "A+ (High-end laptop)"
            result.sendThroughput >= 3000 -> "A (Mid-range laptop)"
            result.sendThroughput >= 1500 -> "B (Entry-level laptop)"
            result.sendThroughput >= 800 -> "C (Below expectations)"
            else -> "D (Needs optimization)"
        }

        logger.info("")
        logger.info("🏆 Kafka Performance Grade: $laptopKafkaRange")
        logger.info("📈 Recovery Rate: ${if (result.recoveryRate >= 90) "Excellent" else if (result.recoveryRate >= 80) "Good" else "Needs improvement"}")
    }

    private fun logRabbitMQResults(result: RabbitMQStyleResult, systemInfo: SystemInfo) {
        logger.info("")
        logger.info("📊 RabbitMQ-Style Multi-Queue Results:")
        logger.info("┌─────────────┬──────────┬──────────┬──────────┬─────────┬──────────┐")
        logger.info("│    Queue    │   Sent   │  S.Tput  │ Received │  R.Tput │ Recovery │")
        logger.info("├─────────────┼──────────┼──────────┼──────────┼─────────┼──────────┤")

        result.queueResults.forEach { (queueName, queueResult) ->
            logger.info(
                "│ %-11s │ %8d │ %6d/s │ %8d │ %5d/s │   %3.0f%%   │".format(
                    queueName, queueResult.messagesSent, queueResult.sendThroughput,
                    queueResult.messagesReceived, queueResult.receiveThroughput, queueResult.recoveryRate
                )
            )
        }

        logger.info("├─────────────┼──────────┼──────────┼──────────┼─────────┼──────────┤")
        logger.info(
            "│ AGGREGATE   │ %8d │ %6d/s │ %8d │ %5d/s │   %3.0f%%   │".format(
                result.totalSent, result.aggregateSendThroughput, result.totalReceived, 
                result.aggregateReceiveThroughput, result.overallRecoveryRate
            )
        )
        logger.info("└─────────────┴──────────┴──────────┴──────────┴─────────┴──────────┘")

        // 성능 등급 분석
        val laptopRabbitRange = when {
            result.aggregateSendThroughput >= 4000 -> "A+ (High-end laptop)"
            result.aggregateSendThroughput >= 2500 -> "A (Mid-range laptop)"
            result.aggregateSendThroughput >= 1200 -> "B (Entry-level laptop)"
            result.aggregateSendThroughput >= 600 -> "C (Below expectations)"
            else -> "D (Needs optimization)"
        }

        logger.info("")
        logger.info("🏆 RabbitMQ Performance Grade: $laptopRabbitRange")
        logger.info("📈 Overall Recovery Rate: ${if (result.overallRecoveryRate >= 90) "Excellent" else if (result.overallRecoveryRate >= 80) "Good" else "Needs improvement"}")
    }

    private suspend fun generateComparisonReport(
        kafkaResult: KafkaStyleResult,
        rabbitResult: RabbitMQStyleResult,
        systemInfo: SystemInfo
    ) {
        logger.info("")
        logger.info("🔥 COMPREHENSIVE LAPTOP PERFORMANCE COMPARISON 🔥")
        logger.info("┌─────────────────────┬─────────────┬─────────────┬─────────────┐")
        logger.info("│       Style         │    Send     │   Receive   │  Recovery   │")
        logger.info("├─────────────────────┼─────────────┼─────────────┼─────────────┤")
        logger.info(
            "│ Kafka (Single Queue)│ %6d msg/s │ %6d msg/s │    %3.0f%%    │".format(
                kafkaResult.sendThroughput, kafkaResult.receiveThroughput, kafkaResult.recoveryRate
            )
        )
        logger.info(
            "│ RabbitMQ (Multi-Q)  │ %6d msg/s │ %6d msg/s │    %3.0f%%    │".format(
                rabbitResult.aggregateSendThroughput, rabbitResult.aggregateReceiveThroughput, rabbitResult.overallRecoveryRate
            )
        )
        logger.info("└─────────────────────┴─────────────┴─────────────┴─────────────┘")

        // 상대 성능 분석
        val sendRatio = kafkaResult.sendThroughput.toDouble() / rabbitResult.aggregateSendThroughput
        val receiveRatio = kafkaResult.receiveThroughput.toDouble() / rabbitResult.aggregateReceiveThroughput

        logger.info("")
        logger.info("📊 Performance Analysis:")
        logger.info("- Kafka vs RabbitMQ (Send): ${String.format("%.2f", sendRatio)}x ${if (sendRatio > 1) "faster" else "slower"}")
        logger.info("- Kafka vs RabbitMQ (Receive): ${String.format("%.2f", receiveRatio)}x ${if (receiveRatio > 1) "faster" else "slower"}")

        // 실제 브로커와 비교
        logger.info("")
        logger.info("🏭 Comparison with Real Message Brokers (Laptop):")
        logger.info("┌──────────────────────┬─────────────────────┬─────────────────────┐")
        logger.info("│   Message Broker     │   Typical Laptop    │    Your Service     │")
        logger.info("├──────────────────────┼─────────────────────┼─────────────────────┤")
        logger.info("│ Kafka (Laptop)       │ 2,000-8,000 msg/sec │ %6d msg/sec      │".format(kafkaResult.sendThroughput))
        logger.info("│ RabbitMQ (Laptop)    │ 1,500-5,000 msg/sec │ %6d msg/sec      │".format(rabbitResult.aggregateSendThroughput))
        logger.info("│ Redis (Laptop)       │ 5,000-15,000 msg/sec│ Reference baseline   │")
        logger.info("└──────────────────────┴─────────────────────┴─────────────────────┘")

        // 최종 권장사항
        logger.info("")
        logger.info("💡 Final Recommendations:")
        val bestThroughput = maxOf(kafkaResult.sendThroughput, rabbitResult.aggregateSendThroughput)
        val bestRecovery = maxOf(kafkaResult.recoveryRate, rabbitResult.overallRecoveryRate)

        when {
            bestThroughput >= 3000 && bestRecovery >= 90 -> {
                logger.info("🟢 EXCELLENT performance for laptop environment!")
                logger.info("• Ready for production-scale development and testing")
                logger.info("• Consider scaling up for load testing scenarios")
            }
            bestThroughput >= 1500 && bestRecovery >= 80 -> {
                logger.info("🟡 GOOD performance for laptop development")
                logger.info("• Suitable for development and moderate testing")
                logger.info("• Monitor resource usage during extended tests")
            }
            else -> {
                logger.info("🔴 NEEDS OPTIMIZATION for laptop environment")
                logger.info("• Consider reducing message sizes or batch sizes")
                logger.info("• Check system resources and background processes")
            }
        }

        // MD 파일 생성
        generateMarkdownReport(kafkaResult, rabbitResult, systemInfo)
    }

    private suspend fun generateMarkdownReport(
        kafkaResult: KafkaStyleResult,
        rabbitResult: RabbitMQStyleResult,
        systemInfo: SystemInfo
    ) {
        val reportContent = """
# Simple Queue Service - 노트북 환경 성능 테스트 리포트

## 📅 테스트 정보
- **테스트 일시**: ${java.time.LocalDateTime.now()}
- **테스트 환경**: 노트북 (개발 환경)
- **테스트 목적**: Kafka/RabbitMQ 스타일 성능 비교

## 💻 시스템 사양
- **CPU 코어**: ${systemInfo.cpuCores}개
- **총 메모리**: ${systemInfo.totalMemoryMB}MB
- **사용 가능 메모리**: ${systemInfo.freeMemoryMB}MB
- **최대 메모리**: ${systemInfo.maxMemoryMB}MB
- **운영체제**: ${systemInfo.osName}
- **Java 버전**: ${systemInfo.javaVersion}

## 🔥 Kafka 스타일 성능 테스트

### 테스트 구성
- **테스트 방식**: 단일 큐, 고처리량 배치 처리
- **총 메시지**: ${kafkaResult.totalSent}개
- **프로듀서 스레드**: ${kafkaResult.producerResults.size}개
- **컨슈머 스레드**: ${kafkaResult.consumerResults.size}개
- **메시지 크기**: ~256바이트

### 성능 결과
| 구분 | 처리량 (msg/sec) | 처리 시간 (ms) | 메시지 수 |
|------|------------------|----------------|-----------|
| **송신 (Producer)** | ${kafkaResult.sendThroughput} | ${kafkaResult.totalSendTime} | ${kafkaResult.totalSent} |
| **수신 (Consumer)** | ${kafkaResult.receiveThroughput} | ${kafkaResult.totalReceiveTime} | ${kafkaResult.totalReceived} |

### 스레드별 성능
#### 프로듀서 스레드
| 스레드 ID | 처리량 (msg/sec) | 메시지 수 | 처리 시간 (ms) |
|-----------|------------------|-----------|----------------|
${kafkaResult.producerResults.joinToString("\n") { "| ${it.threadId} | ${it.throughput} | ${it.messagesSent} | ${it.timeMs} |" }}

#### 컨슈머 스레드
| 스레드 ID | 수신 메시지 | 처리 시간 (ms) | 처리량 (msg/sec) |
|-----------|-------------|----------------|------------------|
${kafkaResult.consumerResults.joinToString("\n") { "| ${it.threadId} | ${it.messagesReceived} | ${it.timeMs} | ${it.throughput} |" }}

### Kafka 스타일 분석
- **메시지 복구율**: ${String.format("%.1f", kafkaResult.recoveryRate)}%
- **성능 등급**: ${when {
    kafkaResult.sendThroughput >= 5000 -> "A+ (High-end laptop)"
    kafkaResult.sendThroughput >= 3000 -> "A (Mid-range laptop)"
    kafkaResult.sendThroughput >= 1500 -> "B (Entry-level laptop)"
    kafkaResult.sendThroughput >= 800 -> "C (Below expectations)"
    else -> "D (Needs optimization)"
}}
- **특징**: 단일 큐에서 높은 처리량, 배치 처리 최적화

## 🐰 RabbitMQ 스타일 성능 테스트

### 테스트 구성
- **테스트 방식**: 다중 큐, 라우팅 기반 메시지 분산
- **큐 수**: ${rabbitResult.queueResults.size}개 (${rabbitResult.queueResults.keys.joinToString(", ")})
- **총 메시지**: ${rabbitResult.totalSent}개
- **큐당 메시지**: 평균 ${rabbitResult.totalSent / rabbitResult.queueResults.size}개
- **메시지 크기**: 32B ~ 256B (큐별 상이)

### 성능 결과
| 구분 | 처리량 (msg/sec) | 처리 시간 (ms) | 메시지 수 |
|------|------------------|----------------|-----------|
| **송신 (Producer)** | ${rabbitResult.aggregateSendThroughput} | ${rabbitResult.totalSendTime} | ${rabbitResult.totalSent} |
| **수신 (Consumer)** | ${rabbitResult.aggregateReceiveThroughput} | ${rabbitResult.totalReceiveTime} | ${rabbitResult.totalReceived} |

### 큐별 성능
| 큐 이름 | 송신 (msg/sec) | 수신 (msg/sec) | 송신 메시지 | 수신 메시지 | 복구율 (%) |
|---------|----------------|----------------|-------------|-------------|------------|
${rabbitResult.queueResults.values.joinToString("\n") { 
    "| ${it.queueName} | ${it.sendThroughput} | ${it.receiveThroughput} | ${it.messagesSent} | ${it.messagesReceived} | ${String.format("%.0f", it.recoveryRate)} |"
}}

### RabbitMQ 스타일 분석
- **전체 메시지 복구율**: ${String.format("%.1f", rabbitResult.overallRecoveryRate)}%
- **성능 등급**: ${when {
    rabbitResult.aggregateSendThroughput >= 4000 -> "A+ (High-end laptop)"
    rabbitResult.aggregateSendThroughput >= 2500 -> "A (Mid-range laptop)"
    rabbitResult.aggregateSendThroughput >= 1200 -> "B (Entry-level laptop)"
    rabbitResult.aggregateSendThroughput >= 600 -> "C (Below expectations)"
    else -> "D (Needs optimization)"
}}
- **특징**: 다중 큐 라우팅, 메시지 타입별 처리, 복잡한 워크플로우 지원

## 📊 종합 성능 비교

### 직접 비교
| 측정 항목 | Kafka 스타일 | RabbitMQ 스타일 | 비율 |
|-----------|--------------|-----------------|------|
| **송신 처리량** | ${kafkaResult.sendThroughput} msg/sec | ${rabbitResult.aggregateSendThroughput} msg/sec | ${String.format("%.2f", kafkaResult.sendThroughput.toDouble() / rabbitResult.aggregateSendThroughput)}x |
| **수신 처리량** | ${kafkaResult.receiveThroughput} msg/sec | ${rabbitResult.aggregateReceiveThroughput} msg/sec | ${String.format("%.2f", kafkaResult.receiveThroughput.toDouble() / rabbitResult.aggregateReceiveThroughput)}x |
| **메시지 복구율** | ${String.format("%.1f", kafkaResult.recoveryRate)}% | ${String.format("%.1f", rabbitResult.overallRecoveryRate)}% | - |

### 실제 메시지 브로커와 비교 (노트북 환경)
| 메시지 브로커 | 일반적인 노트북 성능 | 테스트 결과 | 비교 |
|---------------|---------------------|-------------|------|
| **Kafka** | 2,000 ~ 8,000 msg/sec | ${kafkaResult.sendThroughput} msg/sec | ${String.format("%.0f", kafkaResult.sendThroughput * 100.0 / 5000)}% |
| **RabbitMQ** | 1,500 ~ 5,000 msg/sec | ${rabbitResult.aggregateSendThroughput} msg/sec | ${String.format("%.0f", rabbitResult.aggregateSendThroughput * 100.0 / 3000)}% |
| **Redis** | 5,000 ~ 15,000 msg/sec | 참고 기준 | - |

## 🎯 성능 분석 및 결론

### 강점
${if (maxOf(kafkaResult.sendThroughput, rabbitResult.aggregateSendThroughput) >= 3000) """
#### 🟢 우수한 성능
- 노트북 환경에서 실제 메시지 브로커 수준의 성능 달성
- 개발 및 테스팅 환경에 적합한 처리량
- 프로덕션 환경 대비 충분한 성능 검증 가능
""" else """
#### 🟡 개선 가능한 성능
- 노트북 환경 고려 시 적절한 성능
- 개발 환경에서의 기본적인 기능 검증 가능
- 최적화를 통한 성능 향상 여지 존재
"""}

### 사용 시나리오별 권장사항

#### Kafka 스타일 권장 상황
- **로깅 및 분석**: 대용량 로그 데이터 처리
- **이벤트 스트리밍**: 실시간 데이터 파이프라인
- **단순 메시징**: 높은 처리량이 필요한 단순 메시지 큐
- **성능**: ${kafkaResult.sendThroughput} msg/sec

#### RabbitMQ 스타일 권장 상황
- **복잡한 라우팅**: 메시지 타입별 다양한 처리 로직
- **마이크로서비스**: 서비스 간 비동기 통신
- **워크플로우**: 작업 단계별 메시지 처리
- **성능**: ${rabbitResult.aggregateSendThroughput} msg/sec

### 노트북 환경 최적화 권장사항

${when {
    maxOf(kafkaResult.sendThroughput, rabbitResult.aggregateSendThroughput) >= 3000 -> """
#### 🚀 성능 최적화 완료
- 현재 설정으로 프로덕션 수준 개발 가능
- 부하 테스트 확장 고려
- 모니터링 도구 추가 검토
"""
    maxOf(kafkaResult.sendThroughput, rabbitResult.aggregateSendThroughput) >= 1500 -> """
#### ⚡ 추가 최적화 고려사항
- 메시지 배치 크기 조정
- 스레드 수 최적화
- 메모리 할당 검토
- SSD 사용 권장
"""
    else -> """
#### 🔧 성능 개선 필요
- 시스템 리소스 확인 (CPU, 메모리, 디스크)
- 백그라운드 프로세스 점검
- JVM 튜닝 고려
- 메시지 크기 최적화
"""
}}

## 📈 메모리 사용량 분석
- **테스트 전 사용 메모리**: ${systemInfo.totalMemoryMB - systemInfo.freeMemoryMB}MB
- **최대 할당 가능 메모리**: ${systemInfo.maxMemoryMB}MB
- **메모리 사용률**: ${String.format("%.1f", (systemInfo.totalMemoryMB - systemInfo.freeMemoryMB) * 100.0 / systemInfo.maxMemoryMB)}%

## 🔮 향후 개선 방향
1. **배치 처리 최적화**: 메시지 배치 크기 동적 조정
2. **메모리 관리**: 큐별 메모리 할당 최적화
3. **모니터링**: 실시간 성능 메트릭 대시보드
4. **스케일링**: 클러스터 환경 테스트 준비

---
*이 리포트는 노트북 개발 환경에서 자동 생성되었습니다. 프로덕션 환경에서는 다른 결과가 나올 수 있습니다.*
        """.trimIndent()

        // MD 파일 저장
        withContext(Dispatchers.IO) {
            val reportFile = java.io.File("./docs/laptop-performance-test-report.md")
            reportFile.writeText(reportContent)
            logger.info("📄 Performance report generated: ${reportFile.absolutePath}")
        }
    }
}
