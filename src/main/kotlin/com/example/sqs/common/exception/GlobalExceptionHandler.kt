package com.example.sqs.common.exception

import com.example.sqs.common.ApiError
import com.example.sqs.common.ApiResponse
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import reactor.core.publisher.Mono

@RestControllerAdvice
class GlobalExceptionHandler {

    private val log = LoggerFactory.getLogger(javaClass)

    @ExceptionHandler(EntityNotFoundException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    fun handleEntityNotFoundException(ex: EntityNotFoundException): ApiResponse<Nothing> {
        log.error("Entity not found: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Entity not found")
    }

    @ExceptionHandler(IllegalArgumentException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleIllegalArgumentException(ex: IllegalArgumentException): ApiResponse<Nothing> {
        log.error("Bad request: {}", ex.message)
        return ApiResponse.error("BAD_REQUEST", ex.message ?: "Invalid request parameters")
    }

    @ExceptionHandler(OperationFailedException::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    fun handleOperationFailedException(ex: OperationFailedException): ApiResponse<Nothing> {
        log.error("Operation failed: {}", ex.message, ex)
        return ApiResponse.error(ex.code, ex.message ?: "Operation failed")
    }

    @ExceptionHandler(AuthenticationException::class)
    @ResponseStatus(HttpStatus.UNAUTHORIZED)
    fun handleAuthenticationException(ex: AuthenticationException): ApiResponse<Nothing> {
        log.error("Authentication failed: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Authentication failed")
    }

    @ExceptionHandler(AccessDeniedException::class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    fun handleAccessDeniedException(ex: AccessDeniedException): ApiResponse<Nothing> {
        log.error("Access denied: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Access denied")
    }

    @ExceptionHandler(DuplicateResourceException::class)
    @ResponseStatus(HttpStatus.CONFLICT)
    fun handleDuplicateResourceException(ex: DuplicateResourceException): ApiResponse<Nothing> {
        log.error("Duplicate resource: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Resource already exists")
    }

    @ExceptionHandler(QuotaExceededException::class)
    @ResponseStatus(HttpStatus.TOO_MANY_REQUESTS)
    fun handleQuotaExceededException(ex: QuotaExceededException): ApiResponse<Nothing> {
        log.error("Quota exceeded: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Quota exceeded")
    }

    @ExceptionHandler(QueueNotFoundException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    fun handleQueueNotFoundException(ex: QueueNotFoundException): ApiResponse<Nothing> {
        log.error("Queue not found: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Queue not found")
    }

    @ExceptionHandler(MessageNotFoundException::class)
    @ResponseStatus(HttpStatus.NOT_FOUND)
    fun handleMessageNotFoundException(ex: MessageNotFoundException): ApiResponse<Nothing> {
        log.error("Message not found: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Message not found")
    }

    @ExceptionHandler(InvalidReceiptHandleException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleInvalidReceiptHandleException(ex: InvalidReceiptHandleException): ApiResponse<Nothing> {
        log.error("Invalid receipt handle: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Invalid receipt handle")
    }

    @ExceptionHandler(MessageTooLargeException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleMessageTooLargeException(ex: MessageTooLargeException): ApiResponse<Nothing> {
        log.error("Message too large: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Message size exceeds limit")
    }

    @ExceptionHandler(ValidationException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleValidationException(ex: ValidationException): ApiResponse<Nothing> {
        log.error("Validation failed: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Validation failed")
    }

    @ExceptionHandler(AuthorizationException::class)
    @ResponseStatus(HttpStatus.FORBIDDEN)
    fun handleAuthorizationException(ex: AuthorizationException): ApiResponse<Nothing> {
        log.error("Authorization failed: {}", ex.message)
        return ApiResponse.error(ex.code, ex.message ?: "Authorization failed")
    }

    @ExceptionHandler(Exception::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    fun handleGenericException(ex: Exception): ApiResponse<Nothing> {
        log.error("Unexpected error occurred: {}", ex.message, ex)
        return ApiResponse.error("INTERNAL_SERVER_ERROR", "An unexpected error occurred")
    }
}
