package com.example.sqs.tenant.domain

import java.time.Instant

data class Tenant(
    val tenantId: String,
    val apiKey: String,
    val name: String,
    val createdAt: Instant,
    val isActive: Boolean = true
)
