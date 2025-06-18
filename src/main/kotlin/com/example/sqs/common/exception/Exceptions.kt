package com.example.sqs.common.exception

/**
 * 엔티티를 찾을 수 없을 때 발생하는 예외
 */
class EntityNotFoundException(
    override val message: String,
    val code: String = "NOT_FOUND"
) : RuntimeException(message)

/**
 * 작업 실패 예외
 */
class OperationFailedException(
    override val message: String,
    val code: String = "OPERATION_FAILED"
) : RuntimeException(message)

/**
 * 인증 실패 예외
 */
class AuthenticationException(
    override val message: String,
    val code: String = "AUTHENTICATION_FAILED"
) : RuntimeException(message)

/**
 * 권한 부족 예외
 */
class AccessDeniedException(
    override val message: String,
    val code: String = "ACCESS_DENIED"
) : RuntimeException(message)

/**
 * 권한 인가 실패 예외 (인증은 성공했지만 권한이 없는 경우)
 */
class AuthorizationException(
    override val message: String,
    val code: String = "AUTHORIZATION_FAILED"
) : RuntimeException(message)

/**
 * 중복된 리소스 예외
 */
class DuplicateResourceException(
    override val message: String,
    val code: String = "DUPLICATE_RESOURCE"
) : RuntimeException(message)

/**
 * 할당량 초과 예외
 */
class QuotaExceededException(
    override val message: String,
    val code: String = "QUOTA_EXCEEDED"
) : RuntimeException(message)

/**
 * 큐를 찾을 수 없는 예외
 */
class QueueNotFoundException(
    override val message: String,
    val code: String = "QUEUE_NOT_FOUND"
) : RuntimeException(message)

/**
 * 메시지를 찾을 수 없는 예외
 */
class MessageNotFoundException(
    override val message: String,
    val code: String = "MESSAGE_NOT_FOUND"
) : RuntimeException(message)

/**
 * 잘못된 Receipt Handle 예외
 */
class InvalidReceiptHandleException(
    override val message: String,
    val code: String = "INVALID_RECEIPT_HANDLE"
) : RuntimeException(message)

/**
 * 메시지 크기 초과 예외
 */
class MessageTooLargeException(
    override val message: String,
    val code: String = "MESSAGE_TOO_LARGE"
) : RuntimeException(message)

/**
 * 파라미터 검증 실패 예외
 */
class ValidationException(
    override val message: String,
    val code: String = "VALIDATION_FAILED"
) : RuntimeException(message)
