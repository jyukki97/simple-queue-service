package com.example.sqs.admin.service

import com.example.sqs.admin.domain.*
import com.example.sqs.common.exception.EntityNotFoundException
import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.repository.TenantRepository
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageImpl
import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Service
import java.time.Instant
import java.util.UUID

@Service
class TenantManagementService(
    private val tenantRepository: TenantRepository,
    private val activityLogService: ActivityLogService
) {

    suspend fun createTenant(request: CreateTenantRequest): TenantResponse {
        val apiKey = generateApiKey()
        val tenant = Tenant(
            tenantId = UUID.randomUUID().toString(),
            apiKey = apiKey,
            name = request.name,
            createdAt = Instant.now(),
            isActive = true
        )

        val savedTenant = tenantRepository.save(tenant).block()
            ?: throw RuntimeException("Failed to save tenant")

        activityLogService.logActivity(
            tenantId = savedTenant.tenantId,
            action = "CREATE_TENANT",
            description = "새 테넌트 '${savedTenant.name}' 생성됨",
            severity = ActivitySeverity.INFO
        ).block()

        return mapToTenantResponse(savedTenant, request.quota)
    }

    suspend fun getTenant(tenantId: String): TenantResponse {
        val tenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        return mapToTenantResponse(tenant, getDefaultQuota())
    }

    suspend fun updateTenant(tenantId: String, request: UpdateTenantRequest): TenantResponse {
        val existingTenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val updatedTenant = existingTenant.copy(
            name = request.name ?: existingTenant.name,
            isActive = request.isActive ?: existingTenant.isActive
        )

        val savedTenant = tenantRepository.save(updatedTenant).block()
            ?: throw RuntimeException("Failed to update tenant")

        activityLogService.logActivity(
            tenantId = savedTenant.tenantId,
            action = "UPDATE_TENANT",
            description = "테넌트 정보 업데이트됨",
            severity = ActivitySeverity.INFO
        ).block()

        return mapToTenantResponse(savedTenant, getDefaultQuota())
    }

    suspend fun deleteTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        tenantRepository.delete(tenant).block()

        activityLogService.logActivity(
            tenantId = tenantId,
            action = "DELETE_TENANT",
            description = "테넌트 삭제됨",
            severity = ActivitySeverity.WARNING
        ).block()
    }

    suspend fun listTenants(pageRequest: PageRequest, status: TenantStatus?): Page<TenantResponse> {
        val allTenants = tenantRepository.findAll().collectList().block() ?: emptyList()

        val filteredTenants = if (status != null) {
            allTenants.filter { tenant ->
                when (status) {
                    TenantStatus.ACTIVE -> tenant.isActive
                    TenantStatus.SUSPENDED -> !tenant.isActive
                    TenantStatus.INACTIVE -> !tenant.isActive
                }
            }
        } else {
            allTenants
        }

        val startIndex = pageRequest.offset.toInt()
        val endIndex = minOf(startIndex + pageRequest.pageSize, filteredTenants.size)
        val pageContent = if (startIndex < filteredTenants.size) {
            filteredTenants.subList(startIndex, endIndex)
                .map { mapToTenantResponse(it, getDefaultQuota()) }
        } else {
            emptyList()
        }

        return PageImpl(pageContent, pageRequest, filteredTenants.size.toLong())
    }

    suspend fun resetApiKey(tenantId: String): TenantResponse {
        val tenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val newApiKey = generateApiKey()
        val updatedTenant = tenant.copy(apiKey = newApiKey)

        val savedTenant = tenantRepository.save(updatedTenant).block()
            ?: throw RuntimeException("Failed to reset API key")

        activityLogService.logActivity(
            tenantId = tenantId,
            action = "RESET_API_KEY",
            description = "API 키가 재설정됨",
            severity = ActivitySeverity.INFO
        ).block()

        return mapToTenantResponse(savedTenant, getDefaultQuota())
    }

    suspend fun getTenantUsage(tenantId: String): TenantUsage {
        // Mock implementation for now
        return TenantUsage(
            tenantId = tenantId,
            messagesSent = (100..10000).random().toLong(),
            messagesReceived = (90..9500).random().toLong(),
            activeQueues = (1..20).random(),
            storageUsedMB = (1.0..100.0).random(),
            apiCallsToday = (100..5000).random().toLong(),
            lastActivity = Instant.now().minusSeconds((1..86400).random().toLong())
        )
    }

    suspend fun getTenantQuota(tenantId: String): TenantQuota {
        // For now, return default quota. In real implementation, this would be stored per tenant
        return getDefaultQuota()
    }

    suspend fun updateTenantQuota(tenantId: String, quota: TenantQuota): TenantQuota {
        // Mock implementation - in real version, this would be stored
        activityLogService.logActivity(
            tenantId = tenantId,
            action = "UPDATE_QUOTA",
            description = "테넌트 할당량 업데이트됨",
            severity = ActivitySeverity.INFO
        ).block()

        return quota
    }

    suspend fun suspendTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val suspendedTenant = tenant.copy(isActive = false)
        tenantRepository.save(suspendedTenant).block()

        activityLogService.logActivity(
            tenantId = tenantId,
            action = "SUSPEND_TENANT",
            description = "테넌트가 일시정지됨",
            severity = ActivitySeverity.WARNING
        ).block()
    }

    suspend fun activateTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).block()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val activatedTenant = tenant.copy(isActive = true)
        tenantRepository.save(activatedTenant).block()

        activityLogService.logActivity(
            tenantId = tenantId,
            action = "ACTIVATE_TENANT",
            description = "테넌트가 활성화됨",
            severity = ActivitySeverity.INFO
        ).block()
    }

    private fun generateApiKey(): String {
        return "sqs_" + UUID.randomUUID().toString().replace("-", "")
    }

    private fun mapToTenantResponse(tenant: Tenant, quota: TenantQuota): TenantResponse {
        return TenantResponse(
            tenantId = tenant.tenantId,
            name = tenant.name,
            email = "${tenant.name.lowercase().replace(" ", ".")}@example.com", // Mock email
            status = if (tenant.isActive) TenantStatus.ACTIVE else TenantStatus.SUSPENDED,
            apiKey = tenant.apiKey,
            quota = quota,
            createdAt = tenant.createdAt,
            updatedAt = tenant.createdAt // Mock updated time
        )
    }

    private fun getDefaultQuota(): TenantQuota {
        return TenantQuota(
            maxQueues = 10,
            maxMessagesPerQueue = 1000,
            maxMessageSizeKB = 256,
            maxApiCallsPerMinute = 100
        )
    }
}
