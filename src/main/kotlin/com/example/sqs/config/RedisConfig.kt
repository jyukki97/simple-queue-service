package com.example.sqs.config

import io.lettuce.core.ClientOptions
import io.lettuce.core.TimeoutOptions
import io.lettuce.core.resource.ClientResources
import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.connection.lettuce.LettuceClientConfiguration
import org.springframework.data.redis.connection.lettuce.LettuceConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Component
import java.time.Duration

@Configuration
@EnableConfigurationProperties(QueueProperties::class, RedisPerformanceProperties::class)
class RedisConfig(
    private val redisPerformanceProperties: RedisPerformanceProperties
) {
    
    @Bean
    @Primary
    fun clientResources(): ClientResources {
        return ClientResources.builder()
            .ioThreadPoolSize(redisPerformanceProperties.ioThreadPoolSize)
            .computationThreadPoolSize(redisPerformanceProperties.computationThreadPoolSize)
            .build()
    }
    
    @Bean
    @Primary
    fun lettuceClientConfiguration(clientResources: ClientResources): LettuceClientConfiguration {
        val timeoutOptions = TimeoutOptions.builder()
            .fixedTimeout(Duration.ofMillis(redisPerformanceProperties.commandTimeoutMs))
            .build()
            
        val clientOptions = ClientOptions.builder()
            .timeoutOptions(timeoutOptions)
            .build()
            
        return LettuceClientConfiguration.builder()
            .clientOptions(clientOptions)
            .clientResources(clientResources)
            .commandTimeout(Duration.ofMillis(redisPerformanceProperties.commandTimeoutMs))
            .build()
    }
    
    @Bean
    fun reactiveRedisTemplate(
        connectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, Any> {
        val serializationContext = RedisSerializationContext
            .newSerializationContext<String, Any>(StringRedisSerializer())
            .key(StringRedisSerializer())
            .value(GenericJackson2JsonRedisSerializer())
            .hashKey(StringRedisSerializer())
            .hashValue(GenericJackson2JsonRedisSerializer())
            .build()
            
        return ReactiveRedisTemplate(connectionFactory, serializationContext)
    }
    
    @Bean
    @Primary
    fun stringReactiveRedisTemplate(
        connectionFactory: ReactiveRedisConnectionFactory
    ): ReactiveRedisTemplate<String, String> {
        val serializationContext = RedisSerializationContext
            .newSerializationContext<String, String>(StringRedisSerializer())
            .key(StringRedisSerializer())
            .value(StringRedisSerializer())
            .hashKey(StringRedisSerializer())
            .hashValue(StringRedisSerializer())
            .build()
            
        return ReactiveRedisTemplate(connectionFactory, serializationContext)
    }
}

@Component
@ConfigurationProperties(prefix = "app.queue")
data class QueueProperties(
    var defaultVisibilityTimeout: Int = 30,
    var maxMessageSize: Int = 262144,
    var maxMessagesPerBatch: Int = 10,
    var schedulerInterval: Long = 30000,
    var defaultMessageRetentionPeriod: Int = 1209600,
    var maxReceiveCount: Int = 3,
    var enableCompression: Boolean = true,
    var compressionThreshold: Int = 1024,
    var partitionCount: Int = 4,
    var enablePartitioning: Boolean = true
)

@Component
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
