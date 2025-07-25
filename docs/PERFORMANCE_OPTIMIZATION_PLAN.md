# Simple Queue Service Performance Optimization Plan

## Current Performance Issues

### Benchmark Results Analysis
- **Current Throughput**: 500-1,000 msg/s
- **Expected for Redis**: 50,000+ msg/s (industry standard)
- **Performance Gap**: 50-100x underperformance

### Root Cause Analysis

#### 1. Multiple Redis Round Trips (Critical Issue)
**Current Code**: 4 separate Redis operations per message
```kotlin
// HotStorageService.saveMessage() - INEFFICIENT
redisTemplate.opsForValue().set(messageKey, messageJson, ttl).awaitSingle()     // 1
redisTemplate.opsForList().rightPush(queueKey, message.messageId).awaitSingle() // 2  
redisTemplate.expire(queueKey, ttl).awaitSingle()                              // 3
redisTemplate.opsForZSet().add(indexKey, message.messageId, timestamp).awaitSingle() // 4
```

**Impact**: 4x network latency penalty per message operation.

#### 2. Synchronous Batch Processing
**Current Code**: Sequential processing in loops
```kotlin
messages.forEach { message ->
    if (saveMessage(message)) { // Each = 4 Redis calls!
        successful++
    }
}
```

#### 3. JSON Serialization Overhead
Multiple serialize/deserialize operations per message through the pipeline.

#### 4. HTTP Connection Overhead
Benchmark uses curl with new connections vs. persistent HTTP/2 connections.

## Optimization Strategy

### Phase 1: Redis Pipeline Optimization (Expected: 10-20x improvement)

#### 1.1 Implement Redis Pipelining
```kotlin
// OPTIMIZED: Single pipeline for all operations
suspend fun saveMessageOptimized(message: Message): Boolean {
    return redisTemplate.execute { connection ->
        connection.multi()  // Start transaction
        
        // Batch all operations
        connection.set(messageKey.toByteArray(), messageJson.toByteArray())
        connection.expire(messageKey.toByteArray(), ttlSeconds)
        connection.rpush(queueKey.toByteArray(), messageId.toByteArray())
        connection.expire(queueKey.toByteArray(), ttlSeconds)
        connection.zadd(indexKey.toByteArray(), timestamp, messageId.toByteArray())
        connection.expire(indexKey.toByteArray(), ttlSeconds)
        
        connection.exec()   // Execute all at once
    }.awaitSingle() != null
}
```

#### 1.2 True Batch Operations
```kotlin
suspend fun saveBatchOptimized(messages: List<Message>): BatchResult {
    return redisTemplate.execute { connection ->
        connection.multi()
        
        messages.forEach { message ->
            // All operations for all messages in single pipeline
            connection.set(...)
            connection.rpush(...)
            connection.zadd(...)
        }
        
        connection.exec()
    }.awaitSingle()
}
```

### Phase 2: Connection Pool Optimization (Expected: 2-3x improvement)

#### 2.1 Optimize Redis Configuration
```yaml
spring:
  redis:
    lettuce:
      pool:
        max-active: 50        # Increase from 20
        max-idle: 20          # Increase from 8  
        min-idle: 10          # Increase from 2
        max-wait: 500ms       # Reduce from 1000ms
      cluster:
        refresh:
          adaptive: true      # Enable adaptive refresh
          period: 30s
```

#### 2.2 Implement HTTP/2 with Connection Pooling
Replace curl-based benchmarks with persistent HTTP/2 connections.

### Phase 3: Serialization Optimization (Expected: 2x improvement)

#### 3.1 Binary Serialization
Replace JSON with more efficient binary formats:
```kotlin
// Use MessagePack or Protocol Buffers instead of JSON
private val messagePackMapper = MessagePack.newDefaultObjectMapper()

suspend fun saveMessage(message: Message): Boolean {
    val messageBytes = messagePackMapper.writeValueAsBytes(message)  // Faster than JSON
    // ... Redis operations
}
```

#### 3.2 Message Caching
Cache frequently accessed messages to avoid repeated serialization.

### Phase 4: Reactive Optimization (Expected: 1.5-2x improvement)

#### 4.1 Optimize Thread Pools
```yaml
app:
  redis:
    performance:
      io-thread-pool-size: 16     # Increase from 4
      computation-thread-pool-size: 8  # Match CPU cores
      enable-pipelining: true
      pipeline-buffer-size: 5000  # Increase from 1000
```

#### 4.2 Non-blocking Operations
Ensure all operations are truly non-blocking throughout the pipeline.

## Implementation Priority

### High Priority (Immediate Impact)
1. **Redis Pipelining** - Single biggest performance gain
2. **True Batch Operations** - Critical for throughput scaling
3. **Connection Pool Tuning** - Low-hanging fruit

### Medium Priority (Significant Impact)
4. **Binary Serialization** - Good ROI for message-heavy workloads
5. **HTTP/2 Connection Pooling** - Important for API performance

### Lower Priority (Polish)
6. **Message Caching** - Beneficial for read-heavy workloads  
7. **Advanced Thread Pool Tuning** - Fine-tuning after major optimizations

## Expected Performance Improvements

| Optimization | Current | Target | Improvement |
|--------------|---------|--------|-------------|
| Redis Pipelining | 1,000 msg/s | 15,000 msg/s | 15x |
| Connection Pooling | 15,000 msg/s | 25,000 msg/s | 1.7x |
| Binary Serialization | 25,000 msg/s | 35,000 msg/s | 1.4x |
| HTTP/2 Connections | 35,000 msg/s | 50,000 msg/s | 1.4x |
| **Total Expected** | **1,000 msg/s** | **50,000 msg/s** | **50x** |

## Implementation Code Examples

### Optimized HotStorageService
```kotlin
@Service
class OptimizedHotStorageService(
    private val redisTemplate: ReactiveRedisTemplate<String, ByteArray>,
    private val messagePackMapper: ObjectMapper
) : StorageTierService {

    suspend fun saveMessageBatch(messages: List<Message>): BatchResult {
        return redisTemplate.execute { connection ->
            // Single pipeline for entire batch
            val pipeline = connection.pipelined()
            
            messages.forEach { message ->
                val messageBytes = messagePackMapper.writeValueAsBytes(message)
                val messageKey = getMessageKey(message.messageId)
                val queueKey = getQueueKey(message.tenantId, message.queueName)
                
                // All operations in pipeline
                pipeline.set(messageKey.toByteArray(), messageBytes)
                pipeline.expire(messageKey.toByteArray(), 3600)
                pipeline.rpush(queueKey.toByteArray(), message.messageId.toByteArray())
            }
            
            pipeline.sync()  // Execute all at once
            BatchResult(messages.size, 0, emptyList())
        }.awaitSingle()
    }
}
```

### Connection Pool Configuration
```kotlin
@Configuration
class OptimizedRedisConfig {
    
    @Bean
    @Primary
    fun optimizedRedisTemplate(): ReactiveRedisTemplate<String, ByteArray> {
        val config = LettuceConnectionFactory.createStandaloneConfiguration()
        config.hostName = "localhost"
        config.port = 6379
        
        val clientConfig = LettuceClientConfiguration.builder()
            .commandTimeout(Duration.ofMillis(500))  // Reduced timeout
            .shutdownTimeout(Duration.ofMillis(100))
            .pooling(GenericObjectPoolConfig().apply {
                maxTotal = 50
                maxIdle = 20
                minIdle = 10
                maxWaitMillis = 500
            })
            .build()
            
        val factory = LettuceConnectionFactory(config, clientConfig)
        return ReactiveRedisTemplate(factory, RedisSerializationContext.newSerializationContext<String, ByteArray>().build())
    }
}
```

## Benchmarking Improvements

### Replace curl with Optimized Client
```kotlin
// Use WebClient with connection pooling instead of curl
@Component
class OptimizedBenchmarkClient {
    
    private val webClient = WebClient.builder()
        .clientConnector(
            ReactorClientHttpConnector(
                HttpClient.create()
                    .protocol(HttpProtocol.H2C)  // HTTP/2
                    .option(ChannelOption.CONNECT_TIMEOUT_MILLIS, 1000)
                    .option(ChannelOption.SO_KEEPALIVE, true)
                    .responseTimeout(Duration.ofSeconds(5))
            )
        )
        .build()
    
    suspend fun sendMessages(messages: List<SendMessageRequest>): List<SendMessageResponse> {
        return messages.map { request ->
            webClient.post()
                .uri("/api/v1/queues/test-queue/messages")
                .header("X-API-Key", "test-api-key")
                .bodyValue(request)
                .retrieve()
                .bodyToMono<SendMessageResponse>()
                .awaitSingle()
        }
    }
}
```

## Next Steps

1. **Implement Redis pipelining** (highest impact)
2. **Upgrade benchmark client** to use persistent connections
3. **Re-run benchmarks** to measure improvements
4. **Iterate on remaining optimizations**

Target: Achieve **50,000+ msg/s** throughput (industry-competitive performance).