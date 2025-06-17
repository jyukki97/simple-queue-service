package com.example.sqs.common.controller

import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.common.domain.ErrorCode
import org.springframework.http.HttpStatus
import org.springframework.web.bind.MethodArgumentNotValidException
import org.springframework.web.bind.annotation.ExceptionHandler
import org.springframework.web.bind.annotation.ResponseStatus
import org.springframework.web.bind.annotation.RestControllerAdvice
import org.springframework.web.server.ServerWebInputException

@RestControllerAdvice
class GlobalExceptionHandler {

    @ExceptionHandler(MethodArgumentNotValidException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleValidationException(ex: MethodArgumentNotValidException): ApiResponse<Nothing> {
        val errors = ex.bindingResult.fieldErrors
            .map { "${it.field}: ${it.defaultMessage}" }
            .joinToString(", ")
        return ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "Validation failed: $errors")
    }

    @ExceptionHandler(ServerWebInputException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleServerWebInputException(ex: ServerWebInputException): ApiResponse<Nothing> {
        return ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, "Invalid request: ${ex.reason}")
    }

    @ExceptionHandler(IllegalArgumentException::class)
    @ResponseStatus(HttpStatus.BAD_REQUEST)
    fun handleIllegalArgumentException(ex: IllegalArgumentException): ApiResponse<Nothing> {
        return ApiResponse.error(ErrorCode.INVALID_PARAMETER.name, ex.message ?: "Invalid argument")
    }

    @ExceptionHandler(Exception::class)
    @ResponseStatus(HttpStatus.INTERNAL_SERVER_ERROR)
    fun handleGenericException(ex: Exception): ApiResponse<Nothing> {
        return ApiResponse.error(ErrorCode.INTERNAL_ERROR.name, "Internal server error: ${ex.message}")
    }
}
