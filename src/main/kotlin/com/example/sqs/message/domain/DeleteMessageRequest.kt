package com.example.sqs.message.domain

/**
 * 메시지 삭제 요청
 */
data class DeleteMessageRequest(
    val receiptHandle: String
)
