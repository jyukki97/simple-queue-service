package com.example.sqs.tenant.service

import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.repository.TenantRepository
import org.springframework.stereotype.Service
import reactor.core.publisher.Mono

@Service
class TenantService(
    private val tenantRepository: TenantRepository
) {
    fun findByApiKey(apiKey: String): Mono<Tenant> {
        return tenantRepository.findByApiKey(apiKey)
    }

    fun getTenantId(apiKey: String): Mono<String> {
        return findByApiKey(apiKey)
            .map { it.tenantId }
    }

    fun createTenant(tenantId: String, apiKey: String, name: String): Mono<Tenant> {
        return tenantRepository.save(
            Tenant(
                tenantId = tenantId,
                apiKey = apiKey,
                name = name,
                createdAt = java.time.Instant.now(),
                isActive = true
            )
        )
    }
}
