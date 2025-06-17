package com.example.sqs.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.ReactiveRedisConnectionFactory
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.RedisSerializationContext
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Component

@Configuration
@EnableConfigurationProperties(QueueProperties::class)
class RedisConfig {
    
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
}

@Component
@ConfigurationProperties(prefix = "app.queue")
data class QueueProperties(
    var defaultVisibilityTimeout: Int = 30,
    var maxMessageSize: Int = 262144,
    var maxMessagesPerBatch: Int = 10,
    var schedulerInterval: Long = 30000,
    var defaultMessageRetentionPeriod: Int = 1209600,
    var maxReceiveCount: Int = 3
)
