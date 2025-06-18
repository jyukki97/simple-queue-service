package com.example.sqs.tenant.repository

import com.example.sqs.tenant.domain.Tenant
import reactor.core.publisher.Flux
import reactor.core.publisher.Mono

interface TenantRepository {
    fun findByApiKey(apiKey: String): Mono<Tenant>
    fun findByTenantId(tenantId: String): Mono<Tenant>
    fun findAll(): Flux<Tenant>
    fun save(tenant: Tenant): Mono<Tenant>
    fun delete(tenant: Tenant): Mono<Void>
    fun existsByApiKey(apiKey: String): Mono<Boolean>
    fun existsByTenantId(tenantId: String): Mono<Boolean>
}
