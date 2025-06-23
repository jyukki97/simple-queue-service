package com.example.sqs.config

import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.context.annotation.Configuration

/**
 * 큐 관련 설정 프로퍼티
 */
@Configuration
@ConfigurationProperties(prefix = "app.queue")
data class QueueProperties(
    var defaultVisibilityTimeout: Int = 30,
    var maxMessageSize: Int = 262144, // 256KB
    var maxMessagesPerBatch: Int = 10,
    var schedulerInterval: Long = 30000, // 30초
    var defaultMessageRetentionPeriod: Long = 1209600, // 14일
    var maxReceiveCount: Int = 3,
    var partitionCount: Int = 4,
    var enableHotWarmSwap: Boolean = true,
    var enableCompression: Boolean = true,
    var compressionThreshold: Int = 1024,
    var enablePartitioning: Boolean = true
)

/**
 * Redis 성능 최적화 설정
 */
@Configuration
@ConfigurationProperties(prefix = "app.redis.performance")
data class RedisPerformanceProperties(
    var ioThreadPoolSize: Int = 4,
    var computationThreadPoolSize: Int = 4,
    var commandTimeoutMs: Long = 5000,
    var enablePipelining: Boolean = true,
    var pipelineBufferSize: Int = 1000,
    var maxPoolSize: Int = 20,
    var minIdleSize: Int = 5
)

/**
 * 스토리지 계층화 설정
 */
@Configuration
@ConfigurationProperties(prefix = "app.storage")
data class StorageProperties(
    var hotTtlHours: Long = 1, // HOT 계층 TTL (1시간)
    var warmTtlHours: Long = 24, // WARM 계층 TTL (24시간)
    var coldPath: String = "./data/cold", // COLD 스토리지 경로
    var warmPath: String = "./data/warm", // WARM 스토리지 경로
    var compressionEnabled: Boolean = true,
    var batchSize: Int = 100,
    var enableHotWarmSwap: Boolean = true
)
