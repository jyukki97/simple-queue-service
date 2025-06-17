package com.example.sqs.common.exception

/**
 * 큐를 찾을 수 없을 때 발생하는 예외
 */
class QueueNotFoundException(
    queueName: String,
    message: String = "Queue not found: $queueName"
) : RuntimeException(message)
