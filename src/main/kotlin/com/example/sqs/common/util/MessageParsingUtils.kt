package com.example.sqs.common.util

import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import java.time.Instant

/**
 * 메시지 파싱 관련 공통 유틸리티
 */
object MessageParsingUtils {
    
    private val logger = LoggerFactory.getLogger(MessageParsingUtils::class.java)
    private val objectMapper = ObjectMapper()
    
    /**
     * JSON 문자열에서 메시지 데이터를 파싱
     */
    fun parseMessageData(messageJson: String): Map<String, Any>? {
        return try {
            objectMapper.readValue(messageJson, Map::class.java) as Map<String, Any>
        } catch (e: Exception) {
            logger.warn("메시지 JSON 파싱 실패: $messageJson", e)
            null
        }
    }
    
    /**
     * 메시지 데이터에서 타임스탬프를 파싱
     */
    fun parseTimestamp(messageData: Map<String, Any>, fieldName: String = "sentTimestamp"): Instant {
        val timestamp = messageData[fieldName]
        return parseTimestampValue(timestamp)
    }
    
    /**
     * 타임스탬프 값을 Instant로 변환
     */
    fun parseTimestampValue(timestamp: Any?): Instant {
        return when (timestamp) {
            is Number -> {
                try {
                    val seconds = timestamp.toDouble()
                    Instant.ofEpochSecond(seconds.toLong(), ((seconds % 1) * 1_000_000_000).toLong())
                } catch (e: Exception) {
                    logger.warn("숫자 타임스탬프 파싱 실패: $timestamp", e)
                    Instant.now()
                }
            }
            
            is String -> {
                try {
                    // Unix timestamp (seconds with decimal)
                    if (timestamp.contains(".") && timestamp.matches(Regex("\\d+\\.\\d+"))) {
                        val seconds = timestamp.toDouble()
                        Instant.ofEpochSecond(seconds.toLong(), ((seconds % 1) * 1_000_000_000).toLong())
                    } else {
                        // ISO 8601 format
                        Instant.parse(timestamp)
                    }
                } catch (e: Exception) {
                    logger.warn("문자열 타임스탬프 파싱 실패: $timestamp", e)
                    Instant.now()
                }
            }
            
            else -> {
                logger.warn("알 수 없는 타임스탬프 형식: $timestamp")
                Instant.now()
            }
        }
    }
    
    /**
     * 메시지 JSON에서 특정 필드 값을 추출
     */
    fun extractField(messageJson: String, fieldName: String): String? {
        return parseMessageData(messageJson)?.get(fieldName)?.toString()
    }
    
    /**
     * 메시지 JSON에서 attributes 맵을 추출
     */
    fun extractAttributes(messageJson: String): Map<String, String> {
        return try {
            val messageData = parseMessageData(messageJson) ?: return emptyMap()
            val attributesMap = messageData["attributes"] as? Map<String, Any> ?: return emptyMap()
            attributesMap.mapValues { it.value.toString() }
        } catch (e: Exception) {
            logger.warn("attributes 추출 실패: $messageJson", e)
            emptyMap()
        }
    }
    
    /**
     * 메시지 크기 계산 (대략적)
     */
    fun calculateMessageSize(messageJson: String): Long {
        return try {
            val messageData = parseMessageData(messageJson) ?: return 0L
            val body = messageData["messageBody"]?.toString() ?: ""
            body.toByteArray(Charsets.UTF_8).size.toLong()
        } catch (e: Exception) {
            logger.warn("메시지 크기 계산 실패: $messageJson", e)
            messageJson.length.toLong() // fallback
        }
    }
}
