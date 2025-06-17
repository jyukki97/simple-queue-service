package com.example.sqs.tenant.service

import com.example.sqs.tenant.domain.Tenant
import com.example.sqs.tenant.repository.TenantRepository
import org.springframework.stereotype.Service

@Service
class TenantService(
    private val tenantRepository: TenantRepository
) {
    
    fun findByApiKey(apiKey: String): Tenant? {
        return tenantRepository.findByApiKey(apiKey)
    }
    
    fun validateApiKey(apiKey: String): Boolean {
        val tenant = findByApiKey(apiKey)
        return tenant?.isActive == true
    }
    
    fun getTenantId(apiKey: String): String? {
        return findByApiKey(apiKey)?.tenantId
    }
}
