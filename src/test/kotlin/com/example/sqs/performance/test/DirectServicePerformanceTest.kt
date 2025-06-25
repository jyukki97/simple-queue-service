package com.example.sqs.performance.test

import com.example.sqs.message.domain.ReceiveMessageRequest
import com.example.sqs.message.domain.SendMessageRequest
import com.example.sqs.message.service.MessageService
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.service.QueueService
import com.example.sqs.storage.service.WarmTierManagerService
import com.example.sqs.test.base.BaseIntegrationTest
import kotlinx.coroutines.*
import kotlinx.coroutines.sync.Semaphore
import org.junit.jupiter.api.Test
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.context.ApplicationContext
import org.springframework.test.context.TestPropertySource
import kotlin.system.measureTimeMillis

/**
 * 직접 서비스 호출 성능 테스트 - HTTP 오버헤드 제거
 */
@TestPropertySource(
    properties = [
        "sqs.storage.warm.max-size=1000000",
        "sqs.storage.cold.enabled=true",
        "sqs.storage.tier.enabled=true",
        "storage.warm.rocksdb.path=./data/warm-test",
        "app.metrics.enable-prometheus=false"
    ]
)
class DirectServicePerformanceTest : BaseIntegrationTest() {

    private val logger = LoggerFactory.getLogger(DirectServicePerformanceTest::class.java)

    @Autowired
    private lateinit var queueService: QueueService

    @Autowired
    private lateinit var messageService: MessageService

    @Autowired
    private lateinit var applicationContext: ApplicationContext

    @Test
    fun `test direct redis performance baseline`() = runBlocking {
        logger.info("=== Testing Direct Redis Performance Baseline ===")

        val messageCount = 10000
        val testData = "test message data for performance testing"

        // Redis 직접 쓰기 테스트
        val directWriteTime = measureTimeMillis {
            repeat(messageCount) { index ->
                redisTemplate.opsForValue().set("perf_test_$index", testData).block()
            }
        }

        // Redis 직접 읽기 테스트
        val directReadTime = measureTimeMillis {
            repeat(messageCount) { index ->
                redisTemplate.opsForValue().get("perf_test_$index").block()
            }
        }

        val writePerf = messageCount * 1000 / directWriteTime
        val readPerf = messageCount * 1000 / directReadTime

        logger.info("📊 Direct Redis Performance:")
        logger.info("- Write: $writePerf msg/sec (${directWriteTime}ms for $messageCount messages)")
        logger.info("- Read: $readPerf msg/sec (${directReadTime}ms for $messageCount messages)")

        // 정리
        repeat(messageCount) { index ->
            redisTemplate.delete("perf_test_$index").block()
        }
    }

    @Test
    fun `test direct service call performance - sequential`() = runBlocking {
        logger.info("=== Testing Direct Service Call Performance (Sequential) ===")

        // 큐 생성
        val queueName = "direct-perf-sequential"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 1000
        val messageBody = "Direct service test message"

        // 순차 전송 테스트
        val sendTime = measureTimeMillis {
            repeat(messageCount) { index ->
                messageService.sendMessage(
                    tenantId = tenantId,
                    queueName = queueName,
                    request = SendMessageRequest(
                        messageBody = "$messageBody #$index",
                        delaySeconds = 0,
                        attributes = mapOf("index" to index.toString())
                    )
                ).block()
            }
        }

        // 순차 수신 테스트
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            repeat(200) { // 최대 200번 시도
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
                    receivedCount += messages.size
                } else {
                    // 연속 빈 응답 체크
                    if (receivedCount > 0) return@repeat
                }
            }
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 Direct Service Performance (Sequential):")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms for $messageCount messages)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms for $receivedCount messages)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
    }

    @Test
    fun `test direct service call performance - parallel send`() = runBlocking {
        logger.info("=== Testing Direct Service Call Performance (Parallel Send) ===")

        // 큐 생성
        val queueName = "direct-perf-parallel"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 1000
        val concurrency = 50 // 50개 동시 처리
        val messageBody = "Parallel service test message"

        // 병렬 전송 테스트
        val sendTime = measureTimeMillis {
            val jobs = (0 until messageCount).chunked(messageCount / concurrency).map { chunk ->
                async(Dispatchers.IO) {
                    chunk.forEach { index ->
                        messageService.sendMessage(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = SendMessageRequest(
                                messageBody = "$messageBody #$index",
                                delaySeconds = 0,
                                attributes = mapOf("index" to index.toString())
                            )
                        ).block()
                    }
                }
            }
            jobs.awaitAll()
        }

        // 병렬 수신 테스트
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            val receiveJobs = (1..10).map { // 10개 동시 수신 작업
                async(Dispatchers.IO) {
                    var localReceived = 0
                    repeat(30) { // 각 작업당 최대 30번 시도
                        val messages = messageService.receiveMessages(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = ReceiveMessageRequest(
                                maxNumberOfMessages = 10,
                                waitTimeSeconds = 1,
                                visibilityTimeoutSeconds = 30
                            )
                        ).collectList().block() ?: emptyList()

                        if (messages.isNotEmpty()) {
                            localReceived += messages.size
                        } else {
                            if (localReceived > 0) return@repeat
                        }
                    }
                    localReceived
                }
            }
            receivedCount = receiveJobs.awaitAll().sum()
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 Direct Service Performance (Parallel):")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms for $messageCount messages)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms for $receivedCount messages)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
        logger.info("- Concurrency level: $concurrency")
    }

    @Test
    fun `test maximum performance with optimized settings`() = runBlocking {
        logger.info("=== Testing Maximum Performance with Optimized Settings ===")

        // 큐 생성
        val queueName = "direct-perf-optimized"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 5000
        val sendConcurrency = 100 // 더 높은 동시성
        val receiveConcurrency = 20
        val messageBody = "Optimized performance test message"

        // 최적화된 병렬 전송
        val sendTime = measureTimeMillis {
            val semaphore = Semaphore(sendConcurrency)
            val jobs = (0 until messageCount).map { index ->
                async(Dispatchers.IO) {
                    semaphore.acquire()
                    try {
                        messageService.sendMessage(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = SendMessageRequest(
                                messageBody = "$messageBody #$index",
                                delaySeconds = 0,
                                attributes = mapOf("index" to index.toString())
                            )
                        ).block()
                    } finally {
                        semaphore.release()
                    }
                }
            }
            jobs.awaitAll()
        }

        // 잠시 대기 (메시지 저장 완료 보장)
        delay(1000)

        // 최적화된 병렬 수신
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            val receiveSemaphore = Semaphore(receiveConcurrency)
            val receiveJobs = (1..receiveConcurrency).map {
                async(Dispatchers.IO) {
                    receiveSemaphore.acquire()
                    try {
                        var localReceived = 0
                        var consecutiveEmpty = 0
                        repeat(100) { // 각 작업당 최대 100번 시도
                            val messages = messageService.receiveMessages(
                                tenantId = tenantId,
                                queueName = queueName,
                                request = ReceiveMessageRequest(
                                    maxNumberOfMessages = 10,
                                    waitTimeSeconds = 0, // 대기 시간 최소화
                                    visibilityTimeoutSeconds = 30
                                )
                            ).collectList().block() ?: emptyList()

                            if (messages.isNotEmpty()) {
                                localReceived += messages.size
                                consecutiveEmpty = 0
                            } else {
                                consecutiveEmpty++
                                if (consecutiveEmpty >= 3) return@repeat
                            }
                        }
                        localReceived
                    } finally {
                        receiveSemaphore.release()
                    }
                }
            }
            receivedCount = receiveJobs.awaitAll().sum()
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 Maximum Performance Results:")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms for $messageCount messages)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms for $receivedCount messages)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
        logger.info("- Send concurrency: $sendConcurrency")
        logger.info("- Receive concurrency: $receiveConcurrency")

        // 성능 비교 로그
        logger.info("🚀 Performance Improvement Analysis:")
        logger.info("- Expected HTTP performance: ~50-150 msg/sec")
        logger.info("- Actual direct service performance: $sendThroughput msg/sec")
        logger.info("- Performance multiplier: ${sendThroughput / 100.0}x")
    }

    @Test
    fun `test hot storage specific performance`() = runBlocking {
        logger.info("=== Testing HOT Storage Specific Performance ===")

        // HOT 스토리지에 직접 저장되도록 작은 메시지 사용
        val queueName = "direct-perf-hot"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 2000
        val concurrency = 50
        val smallMessage = "HOT" // 작은 메시지로 HOT 스토리지 보장

        // HOT 스토리지 전송 성능
        val sendTime = measureTimeMillis {
            val jobs = (0 until messageCount).map { index ->
                async(Dispatchers.IO) {
                    messageService.sendMessage(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = SendMessageRequest(
                            messageBody = "$smallMessage #$index",
                            delaySeconds = 0,
                            attributes = mapOf("tier" to "hot", "index" to index.toString())
                        )
                    ).block()
                }
            }
            jobs.awaitAll()
        }

        // HOT 스토리지 수신 성능 (즉시 수신)
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            val receiveJobs = (1..15).map {
                async(Dispatchers.IO) {
                    var localReceived = 0
                    repeat(50) {
                        val messages = messageService.receiveMessages(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = ReceiveMessageRequest(
                                maxNumberOfMessages = 10,
                                waitTimeSeconds = 0,
                                visibilityTimeoutSeconds = 30
                            )
                        ).collectList().block() ?: emptyList()

                        if (messages.isNotEmpty()) {
                            localReceived += messages.size
                        } else {
                            if (localReceived > 0) return@repeat
                        }
                    }
                    localReceived
                }
            }
            receivedCount = receiveJobs.awaitAll().sum()
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 HOT Storage Performance:")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
        logger.info("- Target: HOT Storage (Redis) only")

        // Redis 성능과 비교
        logger.info("🔥 HOT Storage Analysis:")
        logger.info("- This should be closest to pure Redis performance")
        logger.info("- Any significant difference indicates service layer overhead")
    }

    @Test
    fun `test warm storage specific performance`() = runBlocking {
        logger.info("=== Testing WARM Storage Specific Performance ===")

        // 테스트 전 데이터 디렉토리 상태 확인
        val warmDataPath = "./data/warm"
        val warmDir = java.io.File(warmDataPath)
        logger.info("🔍 Pre-test WARM storage directory check:")
        logger.info("- Path: $warmDataPath")
        logger.info("- Exists: ${warmDir.exists()}")
        if (warmDir.exists()) {
            logger.info("- Files: ${warmDir.listFiles()?.map { it.name }}")
        }

        // WARM 스토리지에 저장되도록 중간 크기 메시지 사용
        val queueName = "direct-perf-warm"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 1000
        val concurrency = 30
        val mediumMessage = "WARM_STORAGE_TEST_" + "x".repeat(800) // ~820바이트로 WARM 스토리지 유도

        logger.info("📝 Test parameters:")
        logger.info("- Message count: $messageCount")
        logger.info("- Message size: ~${mediumMessage.length} bytes")
        logger.info("- Expected storage tier: WARM (RocksDB)")

        // WARM 스토리지 전송 성능
        val sendTime = measureTimeMillis {
            val jobs = (0 until messageCount).map { index ->
                async(Dispatchers.IO) {
                    messageService.sendMessage(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = SendMessageRequest(
                            messageBody = "$mediumMessage #$index",
                            delaySeconds = 0,
                            attributes = mapOf("tier" to "warm", "index" to index.toString())
                        )
                    ).block()
                }
            }
            jobs.awaitAll()
        }

        // 메시지 전송 후 데이터 디렉토리 상태 확인
        logger.info("🔍 Post-send WARM storage directory check:")
        logger.info("- Exists: ${warmDir.exists()}")
        if (warmDir.exists()) {
            val files = warmDir.listFiles()
            logger.info("- File count: ${files?.size ?: 0}")
            files?.forEach { file ->
                logger.info("  - ${file.name} (${file.length()} bytes)")
            }
        }

        // 잠시 대기 (WARM 스토리지 저장 완료 보장)
        delay(2000)

        // 다시 한번 확인
        logger.info("🔍 After delay WARM storage directory check:")
        if (warmDir.exists()) {
            val files = warmDir.listFiles()
            logger.info("- File count: ${files?.size ?: 0}")
            files?.forEach { file ->
                logger.info("  - ${file.name} (${file.length()} bytes)")
            }
        }

        // WARM 스토리지 수신 성능
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            val receiveJobs = (1..12).map {
                async(Dispatchers.IO) {
                    var localReceived = 0
                    var consecutiveEmpty = 0
                    repeat(60) {
                        val messages = messageService.receiveMessages(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = ReceiveMessageRequest(
                                maxNumberOfMessages = 8,
                                waitTimeSeconds = 0,
                                visibilityTimeoutSeconds = 30
                            )
                        ).collectList().block() ?: emptyList()

                        if (messages.isNotEmpty()) {
                            localReceived += messages.size
                            consecutiveEmpty = 0
                        } else {
                            consecutiveEmpty++
                            if (consecutiveEmpty >= 4) return@repeat
                        }
                    }
                    localReceived
                }
            }
            receivedCount = receiveJobs.awaitAll().sum()
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 WARM Storage Performance:")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
        logger.info("- Target: WARM Storage (RocksDB)")
        logger.info("- Message size: ~820 bytes")

        // WARM 스토리지 실제 사용 여부 검증
        val warmStorageUsed = warmDir.exists() && (warmDir.listFiles()?.isNotEmpty() == true)
        logger.info("🔶 WARM Storage Analysis:")
        logger.info("- RocksDB files created: $warmStorageUsed")
        logger.info("- Should be slower than HOT but faster than COLD")
        logger.info("- RocksDB disk I/O overhead expected")

        if (!warmStorageUsed) {
            logger.warn("⚠️  WARNING: WARM storage files not detected!")
            logger.warn("- Messages might be stored in HOT storage instead")
            logger.warn("- Check storage tier configuration and message size thresholds")
        }
    }

    @Test
    fun `test cold storage specific performance`() = runBlocking {
        logger.info("=== Testing COLD Storage Specific Performance ===")

        // COLD 스토리지에 저장되도록 대용량 메시지 사용
        val queueName = "direct-perf-cold"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 500
        val concurrency = 15
        val largeMessage = "COLD_STORAGE_TEST_" + "x".repeat(4000) // ~4KB로 COLD 스토리지 유도

        // COLD 스토리지 전송 성능
        val sendTime = measureTimeMillis {
            val jobs = (0 until messageCount).map { index ->
                async(Dispatchers.IO) {
                    messageService.sendMessage(
                        tenantId = tenantId,
                        queueName = queueName,
                        request = SendMessageRequest(
                            messageBody = "$largeMessage #$index",
                            delaySeconds = 0,
                            attributes = mapOf("tier" to "cold", "index" to index.toString())
                        )
                    ).block()
                }
            }
            jobs.awaitAll()
        }

        // 충분한 대기 (COLD 스토리지 저장 및 인덱싱 완료 보장)
        delay(5000)

        // COLD 스토리지 수신 성능
        var receivedCount = 0
        val receiveTime = measureTimeMillis {
            val receiveJobs = (1..8).map {
                async(Dispatchers.IO) {
                    var localReceived = 0
                    var consecutiveEmpty = 0
                    repeat(80) {
                        val messages = messageService.receiveMessages(
                            tenantId = tenantId,
                            queueName = queueName,
                            request = ReceiveMessageRequest(
                                maxNumberOfMessages = 5,
                                waitTimeSeconds = 0,
                                visibilityTimeoutSeconds = 30
                            )
                        ).collectList().block() ?: emptyList()

                        if (messages.isNotEmpty()) {
                            localReceived += messages.size
                            consecutiveEmpty = 0
                        } else {
                            consecutiveEmpty++
                            if (consecutiveEmpty >= 5) return@repeat
                        }
                    }
                    localReceived
                }
            }
            receivedCount = receiveJobs.awaitAll().sum()
        }

        val sendThroughput = messageCount * 1000 / sendTime
        val receiveThroughput = receivedCount * 1000 / receiveTime

        logger.info("📊 COLD Storage Performance:")
        logger.info("- Send: $sendThroughput msg/sec (${sendTime}ms)")
        logger.info("- Receive: $receiveThroughput msg/sec (${receiveTime}ms)")
        logger.info("- Message recovery rate: ${receivedCount * 100 / messageCount}%")
        logger.info("- Target: COLD Storage (File System)")
        logger.info("- Message size: ~4KB")

        logger.info("🧊 COLD Storage Analysis:")
        logger.info("- Slowest performance expected due to file I/O")
        logger.info("- Optimized for cost-effective long-term storage")
    }

    @Test
    fun `test storage tier performance comparison`() = runBlocking {
        logger.info("=== Testing Storage Tier Performance Comparison ===")

        val tenantId = "test-tenant"
        val baseMessageCount = 200

        // HOT 스토리지 테스트
        logger.info("🔥 Testing HOT Storage...")
        val hotQueueName = "perf-compare-hot"
        queueService.createQueue(tenantId, hotQueueName, QueueAttributes()).block()

        val hotMessage = "HOT" // 작은 메시지
        val hotSendTime = measureTimeMillis {
            repeat(baseMessageCount) { index ->
                messageService.sendMessage(
                    tenantId = tenantId,
                    queueName = hotQueueName,
                    request = SendMessageRequest(
                        messageBody = "$hotMessage #$index",
                        delaySeconds = 0
                    )
                ).block()
            }
        }

        delay(500)
        var hotReceived = 0
        val hotReceiveTime = measureTimeMillis {
            repeat(50) {
                val messages = messageService.receiveMessages(
                    tenantId = tenantId,
                    queueName = hotQueueName,
                    request = ReceiveMessageRequest(maxNumberOfMessages = 10, waitTimeSeconds = 0)
                ).collectList().block() ?: emptyList()

                if (messages.isNotEmpty()) {
                    hotReceived += messages.size
                } else if (hotReceived > 0) {
                    return@repeat
                }
            }
        }

        // WARM 스토리지 테스트
        logger.info("🔶 Testing WARM Storage...")
        val warmQueueName = "perf-compare-warm"
        queueService.createQueue(tenantId, warmQueueName, QueueAttributes()).block()

        val warmMessage = "WARM_" + "x".repeat(800) // 중간 크기 메시지
        val warmSendTime = measureTimeMillis {
            repeat(baseMessageCount) { index ->
                messageService.sendMessage(
                    tenantId = tenantId,
                    queueName = warmQueueName,
                    request = SendMessageRequest(
                        messageBody = "$warmMessage #$index",
                        delaySeconds = 0
                    )
                ).block()
            }
        }

        delay(1500)
        var warmReceived = 0
        val warmReceiveTime = measureTimeMillis {
            repeat(60) {
                val messages = messageService.receiveMessages(
                    tenantId = tenantId,
                    queueName = warmQueueName,
                    request = ReceiveMessageRequest(maxNumberOfMessages = 8, waitTimeSeconds = 0)
                ).collectList().block() ?: emptyList()

                if (messages.isNotEmpty()) {
                    warmReceived += messages.size
                } else if (warmReceived > 0) {
                    return@repeat
                }
            }
        }

        // COLD 스토리지 테스트
        logger.info("🧊 Testing COLD Storage...")
        val coldQueueName = "perf-compare-cold"
        queueService.createQueue(tenantId, coldQueueName, QueueAttributes()).block()

        val coldMessage = "COLD_" + "x".repeat(3000) // 큰 메시지
        val coldSendTime = measureTimeMillis {
            repeat(baseMessageCount) { index ->
                messageService.sendMessage(
                    tenantId = tenantId,
                    queueName = coldQueueName,
                    request = SendMessageRequest(
                        messageBody = "$coldMessage #$index",
                        delaySeconds = 0
                    )
                ).block()
            }
        }

        delay(3000)
        var coldReceived = 0
        val coldReceiveTime = measureTimeMillis {
            repeat(80) {
                val messages = messageService.receiveMessages(
                    tenantId = tenantId,
                    queueName = coldQueueName,
                    request = ReceiveMessageRequest(maxNumberOfMessages = 5, waitTimeSeconds = 0)
                ).collectList().block() ?: emptyList()

                if (messages.isNotEmpty()) {
                    coldReceived += messages.size
                } else if (coldReceived > 0) {
                    return@repeat
                }
            }
        }

        // 성능 비교 결과
        val hotSendThroughput = baseMessageCount * 1000 / hotSendTime
        val hotReceiveThroughput = hotReceived * 1000 / hotReceiveTime
        val warmSendThroughput = baseMessageCount * 1000 / warmSendTime
        val warmReceiveThroughput = warmReceived * 1000 / warmReceiveTime
        val coldSendThroughput = baseMessageCount * 1000 / coldSendTime
        val coldReceiveThroughput = coldReceived * 1000 / coldReceiveTime

        logger.info("📊 Storage Tier Performance Comparison:")
        logger.info("┌─────────────┬─────────────┬─────────────┬─────────────┐")
        logger.info("│   Storage   │    Send     │   Receive   │  Recovery   │")
        logger.info("├─────────────┼─────────────┼─────────────┼─────────────┤")
        logger.info("│ HOT (Redis) │ %4d msg/sec │ %4d msg/sec │    %3d%%    │".format(hotSendThroughput, hotReceiveThroughput, hotReceived * 100 / baseMessageCount))
        logger.info("│ WARM (RDB)  │ %4d msg/sec │ %4d msg/sec │    %3d%%    │".format(warmSendThroughput, warmReceiveThroughput, warmReceived * 100 / baseMessageCount))
        logger.info("│ COLD (File) │ %4d msg/sec │ %4d msg/sec │    %3d%%    │".format(coldSendThroughput, coldReceiveThroughput, coldReceived * 100 / baseMessageCount))
        logger.info("└─────────────┴─────────────┴─────────────┴─────────────┘")

        logger.info("🎯 Performance Analysis:")
        logger.info("- HOT/WARM ratio: ${hotSendThroughput.toDouble() / warmSendThroughput}x (send), ${hotReceiveThroughput.toDouble() / warmReceiveThroughput}x (receive)")
        logger.info("- WARM/COLD ratio: ${warmSendThroughput.toDouble() / coldSendThroughput}x (send), ${warmReceiveThroughput.toDouble() / coldReceiveThroughput}x (receive)")
        logger.info("- HOT/COLD ratio: ${hotSendThroughput.toDouble() / coldSendThroughput}x (send), ${hotReceiveThroughput.toDouble() / coldReceiveThroughput}x (receive)")
    }

    @Test
    fun `test warm storage forced migration`() = runBlocking {
        logger.info("=== Testing WARM Storage with Forced Migration ===")

        val warmDataPath = "./data/warm-test"
        val warmDir = java.io.File(warmDataPath)
        logger.info("🔍 Pre-test WARM storage directory check:")
        logger.info("- Path: $warmDataPath")
        logger.info("- Exists: ${warmDir.exists()}")

        val queueName = "direct-perf-warm-forced"
        val tenantId = "test-tenant"

        queueService.createQueue(
            tenantId, queueName, attributes = QueueAttributes(
                visibilityTimeoutSeconds = 30,
                messageRetentionPeriod = 1209600
            )
        ).block()

        val messageCount = 50
        val mediumMessage = "WARM_FORCED_TEST_" + "x".repeat(800)

        // 1단계: 메시지 전송 (HOT에 저장됨)
        logger.info("📤 Step 1: Sending messages to HOT storage")
        val sentMessageIds = mutableListOf<String>()

        repeat(messageCount) { index ->
            val result = messageService.sendMessage(
                tenantId = tenantId,
                queueName = queueName,
                request = SendMessageRequest(
                    messageBody = "$mediumMessage #$index",
                    delaySeconds = 0,
                    attributes = mapOf("test" to "forced-warm")
                )
            ).block()

            result?.let { sentMessageIds.add(it.messageId) }
        }

        logger.info("✅ Sent ${sentMessageIds.size} messages to HOT storage")

        // 2단계: WARM 스토리지로 강제 이동
        logger.info("🔄 Step 2: Force moving messages to WARM storage")

        // WarmTierManagerService를 직접 사용하여 강제 이동
        val warmTierManager = applicationContext.getBean(WarmTierManagerService::class.java)

        var movedCount = 0
        sentMessageIds.forEach { messageId ->
            val moved = warmTierManager.moveMessageToWarm(messageId, tenantId, queueName).block() ?: false
            if (moved) {
                movedCount++
                logger.debug("✅ Moved message to WARM: $messageId")
            } else {
                logger.warn("❌ Failed to move message to WARM: $messageId")
            }
        }

        logger.info("📊 Migration Results:")
        logger.info("- Total messages: ${sentMessageIds.size}")
        logger.info("- Successfully moved to WARM: $movedCount")
        logger.info("- Move success rate: ${movedCount * 100 / sentMessageIds.size}%")

        // 3단계: WARM 스토리지 파일 확인
        delay(1000) // RocksDB 플러시 대기

        logger.info("🔍 Post-migration WARM storage directory check:")
        logger.info("- Exists: ${warmDir.exists()}")
        if (warmDir.exists()) {
            val files = warmDir.listFiles()
            logger.info("- File count: ${files?.size ?: 0}")
            files?.forEach { file ->
                logger.info("  - ${file.name} (${file.length()} bytes)")
            }
        }

        // 4단계: RocksDB 통계 확인
        val warmStats = warmTierManager.getWarmStats().block()
        logger.info("📈 RocksDB Statistics:")
        logger.info("- Total messages: ${warmStats?.totalMessages ?: 0}")
        logger.info("- Active messages: ${warmStats?.activeMessages ?: 0}")
        logger.info("- Estimated size: ${warmStats?.estimatedSizeBytes ?: 0} bytes")

        // 5단계: WARM에서 메시지 수신 테스트
        logger.info("📥 Step 3: Testing message reception from WARM storage")

        var receivedFromWarm = 0
        val receiveTime = measureTimeMillis {
            repeat(10) {
                val messages = messageService.receiveMessages(
                    tenantId = tenantId,
                    queueName = queueName,
                    request = ReceiveMessageRequest(
                        maxNumberOfMessages = 10,
                        waitTimeSeconds = 0,
                        visibilityTimeoutSeconds = 30
                    )
                ).collectList().block() ?: emptyList()

                receivedFromWarm += messages.size
                if (messages.isEmpty()) return@repeat
            }
        }

        val receiveThroughput = if (receiveTime > 0) receivedFromWarm * 1000 / receiveTime else 0

        logger.info("📊 WARM Storage Reception Performance:")
        logger.info("- Received: $receivedFromWarm messages")
        logger.info("- Time: ${receiveTime}ms")
        logger.info("- Throughput: $receiveThroughput msg/sec")
        logger.info("- Recovery rate: ${receivedFromWarm * 100 / movedCount}%")

        // 검증
        val warmStorageUsed = warmDir.exists() && (warmDir.listFiles()?.isNotEmpty() == true)
        logger.info("🎯 Final Analysis:")
        logger.info("- RocksDB files created: $warmStorageUsed")
        logger.info("- Messages successfully moved to WARM: $movedCount > 0")
        logger.info("- Messages received from WARM: $receivedFromWarm > 0")

        if (warmStorageUsed && movedCount > 0) {
            logger.info("✅ WARM storage test PASSED - RocksDB is working correctly!")
        } else {
            logger.error("❌ WARM storage test FAILED - RocksDB not functioning properly")
        }

        // 성능 기준 검증
        assert(movedCount > 0) { "No messages were moved to WARM storage" }
        assert(warmStorageUsed) { "WARM storage files were not created" }
        assert(receivedFromWarm > 0) { "No messages were received from WARM storage" }
    }

    // ========== KAFKA/RABBITMQ 스타일 엔터프라이즈 성능 테스트 ==========

    @Test
    fun `kafka style laptop optimized benchmark`() = runBlocking {
        logger.info("=== Kafka-Style Laptop-Optimized Benchmark ===")

        // 노트북 환경 최적화된 Kafka 스타일 설정
        val queueName = "kafka-laptop-benchmark"
        val tenantId = "laptop-kafka-test"

        queueService.createQueue(
            tenantId, queueName,
            QueueAttributes(visibilityTimeoutSeconds = 30, messageRetentionPeriod = 1209600)
        ).block()

        // 노트북 환경 고려한 파라미터 조정
        val totalMessages = 5000  // 서버 대비 절반으로 조정
        val batchSize = 50        // 배치 크기 조정
        val producerThreads = 4   // CPU 코어 수 고려 (일반적으로 4-8코어)
        val consumerThreads = 2   // 노트북 리소스 고려
        val messagePayload = "kafka_laptop_" + "x".repeat(256) // 256B로 조정 (네트워크/메모리 고려)

        logger.info("📋 Kafka-Style Laptop Test Configuration:")
        logger.info("- Total messages: $totalMessages (Laptop optimized)")
        logger.info("- Batch size: $batchSize (Network optimized)")
        logger.info("- Producer threads: $producerThreads (CPU core optimized)")
        logger.info("- Consumer threads: $consumerThreads (Memory optimized)")
        logger.info("- Message size: ~${messagePayload.length} bytes (256B for laptop)")
        logger.info("- Target environment: MacBook/Laptop (not production server)")

        // Producer 성능 테스트 (Kafka 스타일 배치 처리)
        val producerResults = mutableListOf<ProducerResult>()
        logger.info("🚀 Starting Kafka-style batch producers...")

        val totalProduceTime = measureTimeMillis {
            val batches = (0 until totalMessages).chunked(batchSize)
            logger.info("📦 Created ${batches.size} batches for parallel processing")

            coroutineScope {
                val producerJobs = batches.chunked(batches.size / producerThreads).mapIndexed { threadIndex, threadBatches ->
                    async(Dispatchers.IO) {
                        var threadTotalSent = 0
                        var threadTotalTime = 0L

                        logger.debug("Producer-$threadIndex: Processing ${threadBatches.size} batches")

                        threadBatches.forEach { batch ->
                            val batchTime = measureTimeMillis {
                                // Kafka 스타일: 배치 내 메시지들을 순차적으로 빠르게 전송
                                batch.forEach { messageIndex ->
                                    messageService.sendMessage(
                                        tenantId = tenantId,
                                        queueName = queueName,
                                        request = SendMessageRequest(
                                            messageBody = "$messagePayload-batch${messageIndex / batchSize}-msg$messageIndex",
                                            delaySeconds = 0,
                                            attributes = mapOf(
                                                "batch_id" to (messageIndex / batchSize).toString(),
                                                "producer_thread" to threadIndex.toString(),
                                                "message_index" to messageIndex.toString(),
                                                "timestamp" to System.currentTimeMillis().toString()
                                            )
                                        )
                                    ).block()
                                }
                            }
                            threadTotalSent += batch.size
                            threadTotalTime += batchTime

                            // 배치 간 매우 짧은 휴식 (노트북 리소스 고려)
                            if (threadBatches.indexOf(batch) % 5 == 0) {
                                delay(1) // 1ms 휴식으로 CPU 과부하 방지
                            }
                        }

                        val throughput = if (threadTotalTime > 0) (threadTotalSent * 1000 / threadTotalTime).toInt() else 0
                        logger.info("Producer-$threadIndex completed: $threadTotalSent msgs in ${threadTotalTime}ms ($throughput msg/sec)")

                        ProducerResult(
                            threadId = threadIndex,
                            messagesSent = threadTotalSent,
                            timeMs = threadTotalTime,
                            throughput = throughput
                        )
                    }
                }
                producerResults.addAll(producerJobs.awaitAll())
            }
        }

        val totalThroughput = (totalMessages * 1000 / totalProduceTime).toInt()

        // 배치 완료 대기
        delay(2000)

        // Consumer 성능 테스트 (병렬 polling)
        val consumerResults = mutableListOf<ConsumerResult>()
        var totalConsumed = 0
        val totalConsumeTime = measureTimeMillis {
            coroutineScope {
                val consumerJobs = (0 until consumerThreads).map { threadIndex ->
                    async(Dispatchers.IO) {
                        var threadConsumed = 0
                        var emptyPolls = 0
                        val maxEmptyPolls = 5

                        while (emptyPolls < maxEmptyPolls) {
                            val messages = messageService.receiveMessages(
                                tenantId = tenantId,
                                queueName = queueName,
                                request = ReceiveMessageRequest(
                                    maxNumberOfMessages = 10,
                                    waitTimeSeconds = 1,
                                    visibilityTimeoutSeconds = 30
                                )
                            ).collectList().block() ?: emptyList()

                            if (messages.isNotEmpty()) {
                                threadConsumed += messages.size
                                emptyPolls = 0
                            } else {
                                emptyPolls++
                            }
                        }

                        ConsumerResult(
                            threadId = threadIndex,
                            messagesConsumed = threadConsumed
                        )
                    }
                }
                val results = consumerJobs.awaitAll()
                consumerResults.addAll(results)
                totalConsumed = results.sumOf { it.messagesConsumed }
            }
        }

        val consumeThroughput = (totalConsumed * 1000 / totalConsumeTime).toInt()
        val recoveryRate = totalConsumed * 100.0 / totalMessages

        // Kafka 스타일 결과 리포트
        logger.info("📊 Kafka-Style Benchmark Results:")
        logger.info("┌─────────────────────────────────────────────────────────┐")
        logger.info("│                    PRODUCER METRICS                    │")
        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│ Total Throughput: %6d msg/sec (%d ms for %d msgs) │".format(totalThroughput, totalProduceTime, totalMessages))

        producerResults.forEach { result ->
            logger.info("│ Thread %d:        %6d msg/sec (%d msgs)           │".format(result.threadId, result.throughput, result.messagesSent))
        }

        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│                    CONSUMER METRICS                    │")
        logger.info("├─────────────────────────────────────────────────────────┤")
        logger.info("│ Total Throughput: %6d msg/sec (%d ms)              │".format(consumeThroughput, totalConsumeTime))
        logger.info("│ Recovery Rate:    %6.1f%% (%d/%d msgs)             │".format(recoveryRate, totalConsumed, totalMessages))

        consumerResults.forEach { result ->
            logger.info("│ Thread %d:        %6d msgs consumed                │".format(result.threadId, result.messagesConsumed))
        }

        logger.info("└─────────────────────────────────────────────────────────┘")

        // 업계 기준과 비교 (노트북 환경 고려)
        logger.info("🏭 Enterprise Comparison (Laptop-Adjusted):")
        logger.info("- Kafka (Production): ~100K-1M msg/sec")
        logger.info("- Kafka (Laptop):     ~1K-10K msg/sec")
        logger.info("- Your Service:       $totalThroughput msg/sec")

        val laptopKafkaRange = 1000..10000
        val grade = when {
            totalThroughput >= laptopKafkaRange.last -> "A+ (Excellent for laptop)"
            totalThroughput >= laptopKafkaRange.first -> "A (Good for laptop)"
            totalThroughput >= 500 -> "B (Acceptable for laptop)"
            totalThroughput >= 100 -> "C (Below expectations)"
            else -> "D (Poor performance)"
        }

        logger.info("- Performance Grade:  $grade")
        logger.info("- vs Laptop Kafka:    ${totalThroughput * 100 / 5000}% (assuming 5K baseline)")
    }

    @Test
    fun `rabbitmq style multi queue benchmark`() = runBlocking {
        logger.info("=== RabbitMQ-Style Multi-Queue Benchmark ===")

        // RabbitMQ 스타일: 다중 큐, 다중 exchange, routing
        val tenantId = "rabbitmq-test"
        val queues = listOf("orders", "notifications", "analytics", "logs")
        val messageTypes = mapOf(
            "orders" to "order_data_" + "x".repeat(256),
            "notifications" to "notification_" + "x".repeat(128),
            "analytics" to "analytics_event_" + "x".repeat(512),
            "logs" to "log_entry_" + "x".repeat(64)
        )

        // 큐 생성
        queues.forEach { queueName ->
            queueService.createQueue(
                tenantId, queueName,
                QueueAttributes(visibilityTimeoutSeconds = 30, messageRetentionPeriod = 1209600)
            ).block()
        }

        val messagesPerQueue = 2000
        val producersPerQueue = 2
        val consumersPerQueue = 2

        logger.info("📋 RabbitMQ-Style Test Configuration:")
        logger.info("- Queues: ${queues.size} (${queues.joinToString()})")
        logger.info("- Messages per queue: $messagesPerQueue")
        logger.info("- Producers per queue: $producersPerQueue")
        logger.info("- Consumers per queue: $consumersPerQueue")

        // 다중 큐 병렬 생산
        val queueResults = mutableMapOf<String, QueueBenchmarkResult>()
        val totalProduceTime = measureTimeMillis {
            coroutineScope {
                val allProducerJobs = queues.flatMap { queueName ->
                    (0 until producersPerQueue).map { producerIndex ->
                        async(Dispatchers.IO) {
                            val messagePayload = messageTypes[queueName] ?: "default"
                            var sent = 0
                            val time = measureTimeMillis {
                                repeat(messagesPerQueue / producersPerQueue) { messageIndex ->
                                    messageService.sendMessage(
                                        tenantId = tenantId,
                                        queueName = queueName,
                                        request = SendMessageRequest(
                                            messageBody = "$messagePayload-$messageIndex",
                                            delaySeconds = 0,
                                            attributes = mapOf(
                                                "queue" to queueName,
                                                "producer" to producerIndex.toString(),
                                                "type" to queueName
                                            )
                                        )
                                    ).block()
                                    sent++
                                }
                            }
                            QueueProducerResult(queueName, producerIndex, sent, time)
                        }
                    }
                }

                val producerResults = allProducerJobs.awaitAll()
                producerResults.groupBy { it.queueName }.forEach { (queueName, results) ->
                    val totalSent = results.sumOf { it.messagesSent }
                    val avgTime = results.map { it.timeMs }.average()
                    val throughput = (totalSent * 1000 / avgTime).toInt()

                    queueResults[queueName] = QueueBenchmarkResult(
                        queueName = queueName,
                        messagesSent = totalSent,
                        sendThroughput = throughput,
                        messagesConsumed = 0,
                        consumeThroughput = 0
                    )
                }
            }
        }

        delay(3000) // 메시지 전파 대기

        // 다중 큐 병렬 소비
        val totalConsumeTime = measureTimeMillis {
            coroutineScope {
                val allConsumerJobs = queues.flatMap { queueName ->
                    (0 until consumersPerQueue).map { consumerIndex ->
                        async(Dispatchers.IO) {
                            var consumed = 0
                            var emptyPolls = 0

                            while (emptyPolls < 8) {
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
                                } else {
                                    emptyPolls++
                                }
                            }
                            QueueConsumerResult(queueName, consumerIndex, consumed)
                        }
                    }
                }

                val consumerResults = allConsumerJobs.awaitAll()
                consumerResults.groupBy { it.queueName }.forEach { (queueName, results) ->
                    val totalConsumed = results.sumOf { it.messagesConsumed }
                    // totalConsumeTime을 여기서 사용할 수 없으므로 대신 다른 방식으로 계산
                    val avgConsumeThroughput = if (totalConsumed > 0) totalConsumed * 10 else 0 // 임시 계산

                    queueResults[queueName] = queueResults[queueName]!!.copy(
                        messagesConsumed = totalConsumed,
                        consumeThroughput = avgConsumeThroughput
                    )
                }
            }
        }

        // 소비 완료 후 올바른 throughput 계산
        queueResults.forEach { (queueName, result) ->
            val correctConsumeThroughput = if (totalConsumeTime > 0) (result.messagesConsumed * 1000 / totalConsumeTime).toInt() else 0
            queueResults[queueName] = result.copy(consumeThroughput = correctConsumeThroughput)
        }

        // 결과 분석 및 리포트
        val totalSent = queueResults.values.sumOf { it.messagesSent }
        val totalConsumed = queueResults.values.sumOf { it.messagesConsumed }
        val aggregateSendThroughput = (totalSent * 1000 / totalProduceTime).toInt()
        val aggregateConsumeThroughput = (totalConsumed * 1000 / totalConsumeTime).toInt()
        val overallRecoveryRate = totalConsumed * 100.0 / totalSent

        logger.info("📊 RabbitMQ-Style Multi-Queue Results:")
        logger.info("┌─────────────┬──────────┬──────────┬──────────┬─────────┐")
        logger.info("│    Queue    │   Sent   │  S.Tput  │ Consumed │  C.Tput │")
        logger.info("├─────────────┼──────────┼──────────┼──────────┼─────────┤")

        queueResults.forEach { (queueName, result) ->
            val recoveryRate = if (result.messagesSent > 0) result.messagesConsumed * 100 / result.messagesSent else 0
            logger.info(
                "│ %-11s │ %8d │ %6d/s │ %8d │ %5d/s │".format(
                    queueName, result.messagesSent, result.sendThroughput,
                    result.messagesConsumed, result.consumeThroughput
                )
            )
        }

        logger.info("├─────────────┼──────────┼──────────┼──────────┼─────────┤")
        logger.info(
            "│ AGGREGATE   │ %8d │ %6d/s │ %8d │ %5d/s │".format(
                totalSent, aggregateSendThroughput, totalConsumed, aggregateConsumeThroughput
            )
        )
        logger.info("└─────────────┴──────────┴──────────┴──────────┴─────────┘")

        logger.info("📈 Multi-Queue Performance Analysis:")
        logger.info("- Overall Recovery Rate: %.1f%%".format(overallRecoveryRate))
        logger.info("- Average per-queue send: ${queueResults.values.map { it.sendThroughput }.average().toInt()} msg/sec")
        logger.info("- Average per-queue consume: ${queueResults.values.map { it.consumeThroughput }.average().toInt()} msg/sec")

        // RabbitMQ 서버 vs 노트북 비교
        logger.info("🐰 RabbitMQ Server vs Laptop Comparison:")
        logger.info("- RabbitMQ (Production): ~50K-200K msg/sec")
        logger.info("- RabbitMQ (Laptop):     ~2K-8K msg/sec")
        logger.info("- Your Service:          $aggregateSendThroughput msg/sec")

        val laptopRabbitMqRange = 2000..8000
        val rabbitMqGrade = when {
            aggregateSendThroughput >= laptopRabbitMqRange.last -> "A+ (Excellent for laptop)"
            aggregateSendThroughput >= laptopRabbitMqRange.first -> "A (Good for laptop)"
            aggregateSendThroughput >= 1000 -> "B (Acceptable)"
            aggregateSendThroughput >= 500 -> "C (Below expectations)"
            else -> "D (Poor performance)"
        }

        logger.info("- Performance Grade:     $rabbitMqGrade")
        logger.info("- vs Laptop RabbitMQ:    ${aggregateSendThroughput * 100 / 5000}% (assuming 5K baseline)")
    }

    // Supporting data classes
    data class ProducerResult(
        val threadId: Int,
        val messagesSent: Int,
        val timeMs: Long,
        val throughput: Int
    )

    data class ConsumerResult(
        val threadId: Int,
        val messagesConsumed: Int
    )

    data class QueueProducerResult(
        val queueName: String,
        val producerId: Int,
        val messagesSent: Int,
        val timeMs: Long
    )

    data class QueueConsumerResult(
        val queueName: String,
        val consumerId: Int,
        val messagesConsumed: Int
    )

    data class QueueBenchmarkResult(
        val queueName: String,
        val messagesSent: Int,
        val sendThroughput: Int,
        val messagesConsumed: Int,
        val consumeThroughput: Int
    )
}
