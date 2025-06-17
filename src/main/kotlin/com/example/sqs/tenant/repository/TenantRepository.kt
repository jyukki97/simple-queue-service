package com.example.sqs.tenant.repository

import com.example.sqs.tenant.domain.Tenant
import reactor.core.publisher.Mono

interface TenantRepository {
    fun findByApiKey(apiKey: String): Mono<Tenant>
    fun save(tenant: Tenant): Mono<Tenant>
    fun existsByApiKey(apiKey: String): Mono<Boolean>
}
