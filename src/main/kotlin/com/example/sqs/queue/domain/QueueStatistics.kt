package com.example.sqs.queue.domain

import java.time.Instant

/**
 * 큐 통계 정보
 */
data class QueueStatistics(
    val queueName: String,
    val approximateNumberOfMessages: Long,                    // 대기 중인 메시지 수
    val approximateNumberOfMessagesNotVisible: Long,         // 처리 중인 메시지 수 (invisible)
    val approximateNumberOfMessagesInDLQ: Long,              // DLQ 메시지 수  
    val approximateAgeOfOldestMessage: Long?,                // 가장 오래된 메시지 나이 (초)
    val lastUpdated: Instant
)
