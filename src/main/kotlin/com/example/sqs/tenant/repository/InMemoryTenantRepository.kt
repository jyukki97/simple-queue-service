package com.example.sqs.tenant.repository

import com.example.sqs.tenant.domain.Tenant
import org.springframework.stereotype.Repository
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono
import java.time.Instant

@Repository
class InMemoryTenantRepository : TenantRepository {

    private val tenants = mutableMapOf<String, Tenant>()

    init {
        // 테스트용 기본 테넌트 추가
        val defaultTenant = Tenant(
            tenantId = "tenant-1",
            apiKey = "test-api-key",
            name = "Default Tenant",
            createdAt = Instant.now(),
            isActive = true
        )
        tenants[defaultTenant.apiKey] = defaultTenant
    }

    override fun findByApiKey(apiKey: String): Mono<Tenant> {
        return Mono.justOrEmpty(tenants[apiKey])
    }

    override fun findByTenantId(tenantId: String): Mono<Tenant> {
        return Mono.justOrEmpty(tenants.values.find { it.tenantId == tenantId })
    }

    override fun findAll(): Flux<Tenant> {
        return Flux.fromIterable(tenants.values)
    }

    override fun save(tenant: Tenant): Mono<Tenant> {
        tenants[tenant.apiKey] = tenant
        return Mono.just(tenant)
    }

    override fun delete(tenant: Tenant): Mono<Void> {
        tenants.remove(tenant.apiKey)
        return Mono.empty()
    }

    override fun existsByApiKey(apiKey: String): Mono<Boolean> {
        return Mono.just(tenants.containsKey(apiKey))
    }

    override fun existsByTenantId(tenantId: String): Mono<Boolean> {
        return Mono.just(tenants.values.any { it.tenantId == tenantId })
    }
}
