package com.example.sqs.admin.controller

import com.example.sqs.admin.domain.*
import com.example.sqs.admin.service.TenantManagementService
import com.example.sqs.common.ApiResponse
import com.example.sqs.common.exception.EntityNotFoundException
import org.springframework.data.domain.Page
import org.springframework.data.domain.PageRequest
import org.springframework.web.bind.annotation.*
import jakarta.validation.Valid

@RestController
@RequestMapping("/api/v1/admin/tenants")
class TenantManagementController(
    private val tenantManagementService: TenantManagementService
) {
    
    /**
     * 테넌트 목록을 조회합니다.
     */
    @GetMapping
    suspend fun listTenants(
        @RequestParam(defaultValue = "0") page: Int,
        @RequestParam(defaultValue = "10") size: Int,
        @RequestParam(required = false) status: TenantStatus?
    ): ApiResponse<Page<TenantResponse>> {
        val pageRequest = PageRequest.of(page, size)
        val tenants = tenantManagementService.listTenants(pageRequest, status)
        return ApiResponse.success(tenants)
    }
    
    /**
     * 테넌트를 생성합니다.
     */
    @PostMapping
    suspend fun createTenant(
        @Valid @RequestBody request: CreateTenantRequest
    ): ApiResponse<TenantResponse> {
        val tenant = tenantManagementService.createTenant(request)
        return ApiResponse.success(tenant)
    }
    
    /**
     * 테넌트 정보를 조회합니다.
     */
    @GetMapping("/{tenantId}")
    suspend fun getTenant(
        @PathVariable tenantId: String
    ): ApiResponse<TenantResponse> {
        val tenant = tenantManagementService.getTenant(tenantId)
        return ApiResponse.success(tenant)
    }
    
    /**
     * 테넌트 정보를 수정합니다.
     */
    @PutMapping("/{tenantId}")
    suspend fun updateTenant(
        @PathVariable tenantId: String,
        @Valid @RequestBody request: UpdateTenantRequest
    ): ApiResponse<TenantResponse> {
        val tenant = tenantManagementService.updateTenant(tenantId, request)
        return ApiResponse.success(tenant)
    }
    
    /**
     * 테넌트를 삭제합니다.
     */
    @DeleteMapping("/{tenantId}")
    suspend fun deleteTenant(
        @PathVariable tenantId: String
    ): ApiResponse<Unit> {
        tenantManagementService.deleteTenant(tenantId)
        return ApiResponse.success(Unit)
    }
    
    /**
     * 테넌트의 API 키를 재설정합니다.
     */
    @PostMapping("/{tenantId}/reset-api-key")
    suspend fun resetApiKey(
        @PathVariable tenantId: String
    ): ApiResponse<TenantResponse> {
        val tenant = tenantManagementService.resetApiKey(tenantId)
        return ApiResponse.success(tenant)
    }
    
    /**
     * 테넌트의 사용량 정보를 조회합니다.
     */
    @GetMapping("/{tenantId}/usage")
    suspend fun getTenantUsage(
        @PathVariable tenantId: String
    ): ApiResponse<TenantUsage> {
        val usage = tenantManagementService.getTenantUsage(tenantId)
        return ApiResponse.success(usage)
    }
    
    /**
     * 테넌트의 할당량 정보를 조회합니다.
     */
    @GetMapping("/{tenantId}/quota")
    suspend fun getTenantQuota(
        @PathVariable tenantId: String
    ): ApiResponse<TenantQuota> {
        val quota = tenantManagementService.getTenantQuota(tenantId)
        return ApiResponse.success(quota)
    }
    
    /**
     * 테넌트의 할당량을 수정합니다.
     */
    @PutMapping("/{tenantId}/quota")
    suspend fun updateTenantQuota(
        @PathVariable tenantId: String,
        @Valid @RequestBody quota: TenantQuota
    ): ApiResponse<TenantQuota> {
        val updatedQuota = tenantManagementService.updateTenantQuota(tenantId, quota)
        return ApiResponse.success(updatedQuota)
    }
    
    /**
     * 테넌트를 일시 중지합니다.
     */
    @PostMapping("/{tenantId}/suspend")
    suspend fun suspendTenant(
        @PathVariable tenantId: String
    ): ApiResponse<Unit> {
        tenantManagementService.suspendTenant(tenantId)
        return ApiResponse.success(Unit)
    }
    
    /**
     * 테넌트를 활성화합니다.
     */
    @PostMapping("/{tenantId}/activate")
    suspend fun activateTenant(
        @PathVariable tenantId: String
    ): ApiResponse<Unit> {
        tenantManagementService.activateTenant(tenantId)
        return ApiResponse.success(Unit)
    }
}
