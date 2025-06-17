package com.example.sqs.common.domain

import com.fasterxml.jackson.annotation.JsonInclude
import java.time.Instant

@JsonInclude(JsonInclude.Include.NON_NULL)
data class ApiResponse<T>(
    val success: Boolean,
    val data: T? = null,
    val error: ErrorResponse? = null,
    val timestamp: Instant = Instant.now()
) {
    companion object {
        fun <T> success(data: T): ApiResponse<T> = ApiResponse(success = true, data = data)
        fun <T> error(code: String, message: String): ApiResponse<T> = 
            ApiResponse(success = false, error = ErrorResponse(code, message))
    }
}

data class ErrorResponse(
    val code: String,
    val message: String
)

enum class ErrorCode(val message: String) {
    QUEUE_NOT_FOUND("큐를 찾을 수 없습니다"),
    QUEUE_ALREADY_EXISTS("큐가 이미 존재합니다"),
    MESSAGE_NOT_FOUND("메시지를 찾을 수 없습니다"),
    INVALID_RECEIPT_HANDLE("잘못된 receipt handle입니다"),
    MESSAGE_TOO_LARGE("메시지 크기가 너무 큽니다"),
    INVALID_PARAMETER("잘못된 파라미터입니다"),
    UNAUTHORIZED("인증 실패"),
    RATE_LIMIT_EXCEEDED("요청 한도 초과"),
    INTERNAL_ERROR("내부 서버 오류")
}
