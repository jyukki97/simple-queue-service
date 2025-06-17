package com.example.sqs.config

import org.springframework.boot.context.properties.ConfigurationProperties
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.data.redis.connection.RedisConnectionFactory
import org.springframework.data.redis.core.RedisTemplate
import org.springframework.data.redis.serializer.GenericJackson2JsonRedisSerializer
import org.springframework.data.redis.serializer.StringRedisSerializer
import org.springframework.stereotype.Component

@Configuration
@EnableConfigurationProperties(QueueProperties::class)
class RedisConfig {
    
    @Bean
    fun redisTemplate(connectionFactory: RedisConnectionFactory): RedisTemplate<String, Any> {
        return RedisTemplate<String, Any>().apply {
            setConnectionFactory(connectionFactory)
            keySerializer = StringRedisSerializer()
            hashKeySerializer = StringRedisSerializer()
            valueSerializer = GenericJackson2JsonRedisSerializer()
            hashValueSerializer = GenericJackson2JsonRedisSerializer()
            afterPropertiesSet()
        }
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
