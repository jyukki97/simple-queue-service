package com.example.sqs.admin.domain

import java.time.Instant

/**
 * 테넌트 생성 요청 (새로운 버전)
 */
data class CreateTenantRequest(
    val name: String,
    val email: String? = null,
    val quota: TenantQuota = TenantQuota()
)

/**
 * 테넌트 업데이트 요청 (새로운 버전)
 */
data class UpdateTenantRequest(
    val name: String? = null,
    val email: String? = null,
    val isActive: Boolean? = null
)

/**
 * 테넌트 할당량
 */
data class TenantQuota(
    val maxQueues: Int = 10,
    val maxMessagesPerQueue: Long = 1000,
    val maxMessageSizeKB: Int = 256,
    val maxApiCallsPerMinute: Int = 100
)

/**
 * 테넌트 상세 정보 (관리자용)
 */
data class TenantDetailInfo(
    val tenantId: String,
    val apiKey: String,
    val name: String,
    val description: String?,
    val createdAt: Instant,
    val updatedAt: Instant,
    val isActive: Boolean,
    val quotaLimits: TenantQuota,
    val currentUsage: TenantCurrentUsage,
    val lastActivity: Instant?
)

/**
 * 테넌트 현재 사용량
 */
data class TenantCurrentUsage(
    val queueCount: Long,
    val totalMessages: Long,
    val storageUsed: Long,
    val apiCallsToday: Long,
    val apiCallsThisMinute: Long
)

/**
 * 테넌트 목록 조회 요청
 */
data class ListTenantsRequest(
    val limit: Int = 20,
    val offset: Int = 0,
    val namePattern: String? = null,
    val isActive: Boolean? = null,
    val sortBy: TenantSortBy = TenantSortBy.CREATED_AT,
    val sortOrder: SortOrder = SortOrder.DESC
)

enum class TenantSortBy {
    CREATED_AT, NAME, LAST_ACTIVITY, QUEUE_COUNT, MESSAGE_COUNT
}

enum class SortOrder {
    ASC, DESC
}

/**
 * 테넌트 응답
 */
data class TenantResponse(
    val tenantId: String,
    val name: String,
    val email: String,
    val status: TenantStatus,
    val apiKey: String,
    val quota: TenantQuota,
    val createdAt: Instant,
    val updatedAt: Instant
)

/**
 * 테넌트 상태
 */
enum class TenantStatus {
    ACTIVE,
    SUSPENDED,
    INACTIVE
}

/**
 * 테넌트 사용량
 */
data class TenantUsage(
    val tenantId: String,
    val queueCount: Int,
    val messageCount: Long,
    val storageUsedBytes: Long,
    val apiCallsToday: Long,
    val lastActivity: Instant? = null
)
