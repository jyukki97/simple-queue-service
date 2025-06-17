package com.example.sqs.tenant.repository

import com.example.sqs.tenant.domain.Tenant
import org.springframework.stereotype.Repository

@Repository
class InMemoryTenantRepository : TenantRepository {
    
    private val tenants = mutableMapOf<String, Tenant>()
    
    init {
        // 테스트용 기본 테넌트 추가
        val defaultTenant = Tenant(
            tenantId = "tenant-1",
            apiKey = "test-api-key",
            name = "Default Tenant",
            createdAt = java.time.Instant.now(),
            isActive = true
        )
        tenants[defaultTenant.apiKey] = defaultTenant
    }
    
    override fun findByApiKey(apiKey: String): Tenant? {
        return tenants[apiKey]
    }
    
    override fun save(tenant: Tenant): Tenant {
        tenants[tenant.apiKey] = tenant
        return tenant
    }
    
    override fun existsByApiKey(apiKey: String): Boolean {
        return tenants.containsKey(apiKey)
    }
}
