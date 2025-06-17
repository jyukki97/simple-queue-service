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

    @ExceptionHandler(Exception::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    fun handleGenericException(ex: Exception): ApiResponse<Nothing> {
        log.error("Unexpected error occurred: {}", ex.message, ex)
        return ApiResponse.error("INTERNAL_SERVER_ERROR", "An unexpected error occurred")
    }
}
