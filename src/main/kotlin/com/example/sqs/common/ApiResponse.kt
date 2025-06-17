package com.example.sqs.common

import java.time.Instant

/**
 * API 표준 응답 클래스
 */
data class ApiResponse<T>(
    val success: Boolean,
    val data: T? = null,
    val error: ApiError? = null,
    val timestamp: Instant = Instant.now()
) {
    companion object {
        fun <T> success(data: T): ApiResponse<T> {
            return ApiResponse(
                success = true,
                data = data,
                error = null
            )
        }

        fun <T> error(code: String, message: String): ApiResponse<T> {
            return ApiResponse(
                success = false,
                data = null,
                error = ApiError(code, message)
            )
        }
    }
}

/**
 * API 에러 정보
 */
data class ApiError(
    val code: String,
    val message: String
)
