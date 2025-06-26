package com.example.sqs.queue.repository

import com.example.sqs.queue.domain.Queue
import com.example.sqs.queue.domain.QueueAttributes
import com.example.sqs.queue.domain.QueueStatistics
import com.example.sqs.queue.domain.QueueStatsResponse
import com.example.sqs.storage.service.RocksDBWarmStorageService
import com.example.sqs.storage.service.impl.ColdStorageService
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.data.domain.Range
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import reactor.core.scheduler.Schedulers
import java.time.Instant

@Repository
class RedisQueueRepository(
    private val redisTemplate: ReactiveRedisTemplate<String, Any>,
    private val stringRedisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val rocksDBService: RocksDBWarmStorageService,
    private val coldStorageService: ColdStorageService
) : QueueRepository {

    private val logger = LoggerFactory.getLogger(RedisQueueRepository::class.java)

    override fun save(queue: Queue): Mono<Queue> {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        val queueData = mapOf(
            "tenantId" to queue.tenantId,
            "queueName" to queue.queueName,
            "attributes" to objectMapper.writeValueAsString(queue.attributes),
            "createdAt" to queue.createdAt.toString(),
            "updatedAt" to queue.updatedAt.toString()
        )

        return redisTemplate.opsForHash<String, Any>().putAll(key, queueData)
            .then(
                // 테넌트별 큐 목록에도 추가
                redisTemplate.opsForSet().add("tenant:queues:${queue.tenantId}", queue.queueName)
                    .then(Mono.just(queue))
            )
    }

    override fun findByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Queue> {
        val key = "queue:${tenantId}:${queueName}"
        return redisTemplate.opsForHash<String, Any>().entries(key)
            .collectMap(
                { entry -> entry.key },
                { entry -> entry.value }
            )
            .filter { it.isNotEmpty() }
            .map { queueData ->
                val attributes = objectMapper.readValue(
                    queueData["attributes"] as String,
                    QueueAttributes::class.java
                )

                Queue(
                    tenantId = queueData["tenantId"] as String,
                    queueName = queueData["queueName"] as String,
                    attributes = attributes,
                    createdAt = Instant.parse(queueData["createdAt"] as String),
                    updatedAt = Instant.parse(queueData["updatedAt"] as String)
                )
            }
    }

    override fun existsByTenantIdAndQueueName(tenantId: String, queueName: String): Mono<Boolean> {
        val key = "queue:${tenantId}:${queueName}"
        return redisTemplate.hasKey(key)
    }

    override fun findByTenantId(tenantId: String, limit: Int, offset: Int, namePattern: String?): Flux<Queue> {
        val tenantQueuesKey = "tenant:queues:${tenantId}"
        return redisTemplate.opsForSet().members(tenantQueuesKey)
            .cast(String::class.java)
            .filter { queueName ->
                namePattern?.let { pattern ->
                    queueName.matches(pattern.replace("*", ".*").toRegex())
                } ?: true
            }
            .skip(offset.toLong())
            .take(limit.toLong())
            .flatMap { queueName ->
                findByTenantIdAndQueueName(tenantId, queueName)
            }
    }

    override fun findAllQueues(): Flux<Queue> {
        // 모든 테넌트의 큐를 찾기 위해 "tenant:queues:*" 패턴으로 검색
        return redisTemplate.keys("tenant:queues:*")
            .flatMap { tenantQueuesKey ->
                // tenant:queues:tenant-1 형식에서 tenant-1을 추출
                val tenantId = tenantQueuesKey.substringAfterLast(":")

                redisTemplate.opsForSet()
                    .members(tenantQueuesKey)
                    .cast(String::class.java)
                    .flatMap { queueName ->
                        findByTenantIdAndQueueName(tenantId, queueName)
                            .onErrorContinue { error, _ ->
                                // logger 대신 println 사용 (임시)
                                println("큐 로드 실패: tenantId=$tenantId, queueName=$queueName, error=$error")
                            }
                    }
            }
    }

    override fun delete(queue: Queue): Mono<Void> {
        val key = "queue:${queue.tenantId}:${queue.queueName}"
        val tenantQueuesKey = "tenant:queues:${queue.tenantId}"

        // 관련 메시지 큐들도 정리
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()

        return Mono.whenDelayError(
            redisTemplate.delete(key),
            redisTemplate.opsForSet().remove(tenantQueuesKey, queue.queueName),
            redisTemplate.delete(messagesKey),
            redisTemplate.delete(delayedKey),
            redisTemplate.delete(inFlightKey)
        )
    }

    override fun getQueueStats(queue: Queue): Mono<QueueStatsResponse> {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()

        val availableMessagesMono = stringRedisTemplate.opsForList().size(messagesKey).defaultIfEmpty(0L)
        val delayedMessagesMono = stringRedisTemplate.opsForZSet().count(delayedKey, Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
            .defaultIfEmpty(0L)
        val inFlightMessagesMono = stringRedisTemplate.opsForZSet().count(inFlightKey, Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY))
            .defaultIfEmpty(0L)

        return Mono.zip(availableMessagesMono, delayedMessagesMono, inFlightMessagesMono)
            .map { tuple ->
                QueueStatsResponse(
                    availableMessages = tuple.t1.toInt(),
                    inFlightMessages = tuple.t3.toInt(),
                    delayedMessages = tuple.t2.toInt(),
                    deadLetterMessages = 0 // TODO: DLQ 구현 후 추가
                )
            }
    }

    override fun getQueueStatistics(queue: Queue): Mono<QueueStatistics> {
        // 모든 큐 타입에 대해 파티션 기반 통계 집계 (FIFO와 Standard 모두 파티션 사용)
        return getPartitionedQueueStatistics(queue)
    }

    private fun getPartitionedQueueStatistics(queue: Queue): Mono<QueueStatistics> {
        val partitionCount = 4 // @Value로 주입받아야 하지만 여기서는 하드코딩

        // HOT 계층(Redis)의 파티션별 메시지 수 집계
        val hotMessagesMono = Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val hotKey = "queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                stringRedisTemplate.opsForList().size(hotKey).defaultIfEmpty(0L)
            }
            .reduce(0L) { acc, count -> acc + count }
            .defaultIfEmpty(0L)

        // WARM 계층(RocksDB) 메시지 수 조회
        val warmMessagesMono = Mono.fromCallable {
            try {
                val result = rocksDBService.getMessageCountByQueue(queue.tenantId, queue.queueName)
                logger.info("WARM 메시지 수 조회 결과: tenantId=${queue.tenantId}, queueName=${queue.queueName}, count=$result")
                result.toLong()
            } catch (e: Exception) {
                logger.warn("WARM 메시지 수 조회 중 오류 발생: ${e.message}")
                0L
            }
        }.subscribeOn(Schedulers.boundedElastic())

        // COLD 계층(FileSystem) 메시지 수 조회
        val coldMessagesMono = Mono.fromCallable {
            try {
                val result = coldStorageService.getMessageCountByQueue(queue.tenantId, queue.queueName)
                logger.info("COLD 메시지 수 조회 결과: tenantId=${queue.tenantId}, queueName=${queue.queueName}, count=$result")
                result.toLong()
            } catch (e: Exception) {
                logger.warn("COLD 메시지 수 조회 중 오류 발생: ${e.message}")
                0L
            }
        }.subscribeOn(Schedulers.boundedElastic())

        // 지연된 메시지와 처리 중인 메시지는 기존 방식 유지
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()

        val delayedMessagesMono = stringRedisTemplate.opsForZSet().count(
            delayedKey,
            Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
        ).defaultIfEmpty(0L)

        val inFlightMessagesMono = stringRedisTemplate.opsForZSet().count(
            inFlightKey,
            Range.closed(Double.NEGATIVE_INFINITY, Double.POSITIVE_INFINITY)
        ).defaultIfEmpty(0L)

        // 가장 오래된 메시지 조회 (HOT, WARM, COLD 모든 계층에서 비교)
        val hotOldestMono = Flux.range(0, partitionCount)
            .flatMap { partitionNumber ->
                val hotKey = "queue:${queue.tenantId}:${queue.queueName}_partition_${partitionNumber}"
                // HOT에서는 리스트의 끝이 가장 오래된 메시지 (FIFO의 경우)
                stringRedisTemplate.opsForList().index(hotKey, -1)
                    .flatMap { messageId ->
                        stringRedisTemplate.opsForValue().get("message:$messageId")
                            .map { messageJson ->
                                try {
                                    val messageData = objectMapper.readTree(messageJson)
                                    val sentTimestampNode = messageData.get("sentTimestamp")

                                    val sentTime = if (sentTimestampNode.isTextual) {
                                        Instant.parse(sentTimestampNode.asText())
                                    } else {
                                        Instant.ofEpochMilli((sentTimestampNode.asDouble() * 1000).toLong())
                                    }
                                    sentTime
                                } catch (e: Exception) {
                                    Instant.now() // 파싱 실패시 현재 시간 반환
                                }
                            }
                    }
                    .switchIfEmpty(Mono.empty())
            }
            .reduce { oldest1, oldest2 -> if (oldest1.isBefore(oldest2)) oldest1 else oldest2 }
            .switchIfEmpty(Mono.empty())

        // WARM에서 가장 오래된 메시지 조회
        val warmOldestMono = Mono.fromCallable {
            try {
                rocksDBService.getOldestMessageTime(queue.tenantId, queue.queueName)
            } catch (e: Exception) {
                logger.warn("WARM 최오래된 메시지 시간 조회 중 오류 발생: ${e.message}")
                null
            }
        }.subscribeOn(Schedulers.boundedElastic())

        // COLD에서 가장 오래된 메시지 조회
        val coldOldestMono = Mono.fromCallable {
            try {
                coldStorageService.getOldestMessageTime(queue.tenantId, queue.queueName)
            } catch (e: Exception) {
                logger.warn("COLD 최오래된 메시지 시간 조회 중 오류 발생: ${e.message}")
                null
            }
        }.subscribeOn(Schedulers.boundedElastic())

        // HOT, WARM, COLD에서 가장 오래된 메시지 비교
        val oldestMessageMono = Mono.zip(hotOldestMono, warmOldestMono, coldOldestMono)
            .map { tuple ->
                val hotOldest = tuple.t1
                val warmOldest = tuple.t2
                val coldOldest = tuple.t3

                listOfNotNull(hotOldest, warmOldest, coldOldest)
                    .minByOrNull { it }
            }
            .switchIfEmpty(
                // 하나라도 있으면 그걸 사용
                hotOldestMono.switchIfEmpty(warmOldestMono).switchIfEmpty(coldOldestMono)
            )
            .map { oldestTime ->
                Instant.now().epochSecond - (oldestTime?.epochSecond ?: 0L)
            }
            .switchIfEmpty(Mono.empty())

        return Mono.zip(
            hotMessagesMono,
            warmMessagesMono,
            coldMessagesMono,
            delayedMessagesMono,
            inFlightMessagesMono
        ).flatMap { tuple ->
            val totalAvailableMessages = tuple.t1 + tuple.t2 + tuple.t3 // HOT + WARM + COLD

            oldestMessageMono
                .map { age ->
                    QueueStatistics(
                        queueName = queue.queueName,
                        approximateNumberOfMessages = totalAvailableMessages,
                        approximateNumberOfMessagesNotVisible = tuple.t5,
                        approximateNumberOfMessagesInDLQ = 0L, // TODO: DLQ 구현 후 추가
                        approximateAgeOfOldestMessage = age,
                        lastUpdated = Instant.now()
                    )
                }
                .switchIfEmpty(
                    Mono.just(
                        QueueStatistics(
                            queueName = queue.queueName,
                            approximateNumberOfMessages = totalAvailableMessages,
                            approximateNumberOfMessagesNotVisible = tuple.t5,
                            approximateNumberOfMessagesInDLQ = 0L, // TODO: DLQ 구현 후 추가
                            approximateAgeOfOldestMessage = null,
                            lastUpdated = Instant.now()
                        )
                    )
                )
        }
    }

    override fun purgeAllMessages(queue: Queue): Mono<Void> {
        val messagesKey = queue.getMessagesKey()
        val delayedKey = queue.getDelayedKey()
        val inFlightKey = queue.getInFlightKey()

        return Mono.whenDelayError(
            redisTemplate.delete(messagesKey),
            redisTemplate.delete(delayedKey),
            redisTemplate.delete(inFlightKey)
        )
    }

    override fun findAllFifoQueues(): Flux<Queue> {
        return findAllQueues()
            .filter { queue -> queue.attributes.fifoQueue }
    }
}
