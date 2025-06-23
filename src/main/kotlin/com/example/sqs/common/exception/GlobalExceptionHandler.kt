package com.example.sqs.common.exception

import com.example.sqs.common.domain.ApiResponse
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.MethodNotAllowedException
import org.springframework.web.server.NotAcceptableStatusException
import org.springframework.web.server.ResponseStatusException
import org.springframework.web.server.UnsupportedMediaTypeStatusException
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

    @ExceptionHandler(ResponseStatusException::class)
    fun handleResponseStatusException(ex: ResponseStatusException): ApiResponse<Nothing> {
        val status = ex.statusCode
        val reason = ex.reason ?: "Request failed"

        when (status.value()) {
            404 -> {
                log.error("Resource not found: {}", reason)
                return ApiResponse.error("NOT_FOUND", reason)
            }
            400 -> {
                log.error("Bad request: {}", reason)
                return ApiResponse.error("BAD_REQUEST", reason)
            }
            401 -> {
                log.error("Unauthorized: {}", reason)
                return ApiResponse.error("UNAUTHORIZED", reason)
            }
            403 -> {
                log.error("Forbidden: {}", reason)
                return ApiResponse.error("FORBIDDEN", reason)
            }
            405 -> {
                log.error("Method not allowed: {}", reason)
                return ApiResponse.error("METHOD_NOT_ALLOWED", reason)
            }
            else -> {
                log.error("HTTP error {}: {}", status.value(), reason)
                return ApiResponse.error("HTTP_ERROR_${status.value()}", reason)
            }
        }
    }

    @ExceptionHandler(Exception::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    fun handleGenericException(ex: Exception): ApiResponse<Nothing> {
        log.error("Unexpected error occurred - Exception type: {}, Message: {}", ex.javaClass.simpleName, ex.message, ex)
        return ApiResponse.error("INTERNAL_SERVER_ERROR", "An unexpected error occurred")
    }

    @ExceptionHandler(MethodNotAllowedException::class)
    @ResponseStatus(HttpStatus.METHOD_NOT_ALLOWED)
    fun handleMethodNotAllowedException(ex: MethodNotAllowedException): ApiResponse<Nothing> {
        log.error("Method not allowed: {}", ex.message)
        return ApiResponse.error("METHOD_NOT_ALLOWED", "HTTP method not allowed for this endpoint")
    }

    @ExceptionHandler(UnsupportedMediaTypeStatusException::class)
    @ResponseStatus(HttpStatus.UNSUPPORTED_MEDIA_TYPE)
    fun handleUnsupportedMediaTypeException(ex: UnsupportedMediaTypeStatusException): ApiResponse<Nothing> {
        log.error("Unsupported media type: {}", ex.message)
        return ApiResponse.error("UNSUPPORTED_MEDIA_TYPE", "Media type not supported")
    }

    @ExceptionHandler(NotAcceptableStatusException::class)
    @ResponseStatus(HttpStatus.NOT_ACCEPTABLE)
    fun handleNotAcceptableException(ex: NotAcceptableStatusException): ApiResponse<Nothing> {
        log.error("Not acceptable: {}", ex.message)
        return ApiResponse.error("NOT_ACCEPTABLE", "Response format not acceptable")
    }
}
