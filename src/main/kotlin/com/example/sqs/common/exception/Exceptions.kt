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
