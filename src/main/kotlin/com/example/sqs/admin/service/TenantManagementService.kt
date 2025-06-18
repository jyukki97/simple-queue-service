package com.example.sqs.admin.service

import com.example.sqs.admin.domain.*
import com.example.sqs.common.exception.EntityNotFoundException
import com.example.sqs.common.exception.OperationFailedException
import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.repository.TenantRepository
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageImpl
import org.springframework.data.domain.PageRequest
import org.springframework.stereotype.Service
import org.slf4j.LoggerFactory
import kotlinx.coroutines.reactive.awaitSingle
import kotlinx.coroutines.reactive.awaitFirstOrNull
import java.time.Instant
import java.util.UUID
import kotlin.random.Random

@Service
class TenantManagementService(
    private val tenantRepository: TenantRepository
) {

    private val logger = LoggerFactory.getLogger(TenantManagementService::class.java)

    suspend fun createTenant(request: CreateTenantRequest): TenantResponse {
        logger.info("Creating tenant with request: {}", request)
        
        try {
            val apiKey = generateApiKey()
            logger.debug("Generated API key: {}", apiKey)
            
            val tenant = Tenant(
                tenantId = UUID.randomUUID().toString(),
                apiKey = apiKey,
                name = request.name,
                createdAt = Instant.now(),
                isActive = true
            )
            logger.debug("Created tenant entity: {}", tenant)

            val savedTenant = tenantRepository.save(tenant).awaitSingle()
            logger.info("Saved tenant to repository: {}", savedTenant)

            val quota = request.quota ?: getDefaultQuota()
            val email = request.email ?: "${tenant.name.lowercase().replace(" ", ".")}@example.com"
            
            val response = mapToTenantResponse(savedTenant, quota, email)
            logger.info("Created tenant response: {}", response)
            return response
        } catch (e: Exception) {
            logger.error("Error creating tenant: ${e.message}", e)
            throw e
        }
    }

    suspend fun getTenant(tenantId: String): TenantResponse {
        val tenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        return mapToTenantResponse(tenant, getDefaultQuota())
    }

    suspend fun updateTenant(tenantId: String, request: UpdateTenantRequest): TenantResponse {
        val existingTenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val updatedTenant = existingTenant.copy(
            name = request.name ?: existingTenant.name,
            isActive = request.isActive ?: existingTenant.isActive
        )

        val savedTenant = tenantRepository.save(updatedTenant).awaitSingle()

        return mapToTenantResponse(savedTenant, getDefaultQuota())
    }

    suspend fun deleteTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        tenantRepository.delete(tenant).awaitFirstOrNull()
    }

    suspend fun listTenants(pageRequest: PageRequest, status: TenantStatus?): Page<TenantResponse> {
        try {
            val allTenants = tenantRepository.findAll().collectList().awaitSingle()

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
        } catch (e: Exception) {
            // 에러 발생 시 빈 페이지 반환
            println("Error in listTenants: ${e.message}")
            e.printStackTrace()
            return PageImpl(emptyList(), pageRequest, 0)
        }
    }

    suspend fun resetApiKey(tenantId: String): TenantResponse {
        val tenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val newApiKey = generateApiKey()
        val updatedTenant = tenant.copy(apiKey = newApiKey)

        val savedTenant = tenantRepository.save(updatedTenant).awaitSingle()

        return mapToTenantResponse(savedTenant, getDefaultQuota())
    }

    fun getTenantUsage(tenantId: String): TenantUsage {
        // Mock implementation for now
        return TenantUsage(
            tenantId = tenantId,
            messagesSent = (100..10000).random().toLong(),
            messagesReceived = (90..9500).random().toLong(),
            activeQueues = (1..20).random(),
            storageUsedMB = Random.nextDouble(1.0, 100.0),
            apiCallsToday = (100..5000).random().toLong(),
            lastActivity = Instant.now().minusSeconds((1..86400).random().toLong())
        )
    }

    fun getTenantQuota(tenantId: String): TenantQuota {
        // For now, return default quota. In real implementation, this would be stored per tenant
        return getDefaultQuota()
    }

    suspend fun updateTenantQuota(tenantId: String, quota: TenantQuota): TenantQuota {
        return quota
    }

    suspend fun suspendTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val suspendedTenant = tenant.copy(isActive = false)
        tenantRepository.save(suspendedTenant).awaitSingle()
    }

    suspend fun activateTenant(tenantId: String) {
        val tenant = tenantRepository.findByTenantId(tenantId).awaitFirstOrNull()
            ?: throw EntityNotFoundException("테넌트를 찾을 수 없습니다: $tenantId")

        val activatedTenant = tenant.copy(isActive = true)
        tenantRepository.save(activatedTenant).awaitSingle()
    }

    private fun generateApiKey(): String {
        return "sqs_" + UUID.randomUUID().toString().replace("-", "")
    }

    private fun mapToTenantResponse(tenant: Tenant, quota: TenantQuota, email: String? = null): TenantResponse {
        val tenantEmail = email ?: "${tenant.name.lowercase().replace(" ", ".")}@example.com"
        return TenantResponse(
            tenantId = tenant.tenantId,
            name = tenant.name,
            email = tenantEmail,
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
