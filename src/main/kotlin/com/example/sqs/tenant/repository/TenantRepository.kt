package com.example.sqs.tenant.repository

import com.example.sqs.tenant.domain.Tenant

interface TenantRepository {
    fun findByApiKey(apiKey: String): Tenant?
    fun save(tenant: Tenant): Tenant
    fun existsByApiKey(apiKey: String): Boolean
}
