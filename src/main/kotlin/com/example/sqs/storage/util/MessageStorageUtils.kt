package com.example.sqs.storage.util

import com.example.sqs.message.domain.Message
import com.example.sqs.queue.domain.Queue
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.service.StorageStatsService
import com.fasterxml.jackson.databind.ObjectMapper
import org.springframework.data.redis.core.ReactiveRedisTemplate
import org.springframework.stereotype.Component
import reactor.core.publisher.Mono
import java.time.Instant

/**
 * 메시지 저장/조회 관련 공통 유틸리티
 */
@Component
class MessageStorageUtils(
    private val redisTemplate: ReactiveRedisTemplate<String, String>,
    private val objectMapper: ObjectMapper,
    private val storageStatsService: StorageStatsService
) {
    
    /**
     * 메시지 상세 정보를 JSON으로 저장
     */
    fun saveMessageDetails(message: Message): Mono<Void> {
        val messageKey = "message:${message.messageId}"
        val messageJson = objectMapper.writeValueAsString(message)
        
        return redisTemplate.opsForValue()
            .set(messageKey, messageJson)
            .then(
                // 메시지 위치 정보도 함께 저장 (기본적으로 HOT 계층)
                storageStatsService.saveMessageLocation(
                    messageId = message.messageId,
                    tier = StorageTier.HOT,
                    createdAt = message.sentTimestamp
                )
            )
            .then()
    }
    
    /**
     * 메시지 ID로 상세 정보 조회
     */
    fun getMessageDetails(messageId: String): Mono<Message> {
        val messageKey = "message:$messageId"
        
        return redisTemplate.opsForValue()
            .get(messageKey)
            .map { messageJson ->
                objectMapper.readValue(messageJson, Message::class.java)
            }
    }
    
    /**
     * 메시지 파티션 번호 결정 (Standard 큐용)
     */
    fun determinePartitionForStandard(messageBody: String, partitionCount: Int): Int {
        return Math.abs(messageBody.hashCode()) % partitionCount
    }
    
    /**
     * 메시지 파티션 번호 결정 (FIFO 큐용 - MessageGroupId 필수)
     */
    fun determinePartitionForFifo(message: Message, partitionCount: Int): Int {
        val messageGroupId = message.messageGroupId 
            ?: message.attributes["MessageGroupId"]
            ?: throw IllegalStateException("FIFO 큐에서 MessageGroupId가 누락되었습니다")
        return Math.abs(messageGroupId.hashCode()) % partitionCount
    }
    
    /**
     * 타임스탬프 파싱 유틸리티
     */
    fun parseTimestamp(timestampValue: Any?): Instant? {
        return when (timestampValue) {
            is Number -> Instant.ofEpochMilli(timestampValue.toLong())
            is String -> try {
                if (timestampValue.matches(Regex("\\d+"))) {
                    Instant.ofEpochMilli(timestampValue.toLong())
                } else {
                    Instant.parse(timestampValue)
                }
            } catch (e: Exception) {
                null
            }
            else -> null
        }
    }
}

/**
 * 큐 관련 키 생성 확장 함수들 (이미 존재하는 것들)
 */
fun Queue.getHotKey(partition: Int): String = "hot:queue:$tenantId:${queueName}_partition_$partition"
fun Queue.getWarmKey(partition: Int): String = "warm:queue:$tenantId:${queueName}_partition_$partition"
fun Queue.getColdKey(partition: Int): String = "cold:queue:$tenantId:${queueName}_partition_$partition"
fun Queue.getDelayedKey(partition: Int): String = "delayed:queue:$tenantId:${queueName}_partition_$partition"
fun Queue.getInFlightKey(partition: Int): String = "inflight:queue:$tenantId:${queueName}_partition_$partition"
