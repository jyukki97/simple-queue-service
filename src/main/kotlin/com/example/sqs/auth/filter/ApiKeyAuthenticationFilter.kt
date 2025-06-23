package com.example.sqs.auth.filter

import com.example.sqs.auth.domain.AuthContext
import com.example.sqs.common.domain.ApiResponse
import com.example.sqs.tenant.service.TenantService
import com.fasterxml.jackson.databind.ObjectMapper
import org.slf4j.LoggerFactory
import org.springframework.http.HttpStatus
import org.springframework.http.MediaType
import org.springframework.stereotype.Component
import org.springframework.web.server.ServerWebExchange
import org.springframework.web.server.WebFilter
import org.springframework.web.server.WebFilterChain
import reactor.core.publisher.Mono

@Component
class ApiKeyAuthenticationFilter(
    private val tenantService: TenantService,
    private val objectMapper: ObjectMapper
) : WebFilter {
    
    private val logger = LoggerFactory.getLogger(ApiKeyAuthenticationFilter::class.java)
    
    companion object {
        private const val API_KEY_HEADER = "X-API-Key"
        private val EXCLUDED_PATHS = setOf(
            "/actuator",
            "/health",
            "/favicon.ico",
            "/api/v1/ping"
        )
    }
    
    override fun filter(exchange: ServerWebExchange, chain: WebFilterChain): Mono<Void> {
        val request = exchange.request
        val path = request.path.pathWithinApplication().value()
        
        // 제외 경로 체크
        if (EXCLUDED_PATHS.any { path.startsWith(it) }) {
            return chain.filter(exchange)
        }
        
        val apiKey = request.headers.getFirst(API_KEY_HEADER)
        
        if (apiKey.isNullOrBlank()) {
            logger.warn("API Key가 없는 요청: $path")
            return handleAuthError(exchange, "API Key가 필요합니다")
        }
        
        // Admin API Key 체크
        if (apiKey == "admin-api-key") {
            AuthContext.setCurrent(AuthContext(
                tenantId = "admin", 
                apiKey = apiKey, 
                isAdmin = true
            ))
            return chain.filter(exchange)
                .doFinally { AuthContext.clear() }
        }
        
        // 일반 테넌트 API Key 체크
        return tenantService.getTenantId(apiKey)
            .switchIfEmpty(
                Mono.error(RuntimeException("유효하지 않은 API Key입니다"))
            )
            .flatMap { tenantId ->
                AuthContext.setCurrent(AuthContext(
                    tenantId = tenantId,
                    apiKey = apiKey,
                    isAdmin = false
                ))
                
                chain.filter(exchange)
                    .doFinally { AuthContext.clear() }
            }
            .onErrorResume { error ->
                logger.warn("인증 실패: ${error.message}")
                handleAuthError(exchange, error.message ?: "인증에 실패했습니다")
            }
    }
    
    private fun handleAuthError(exchange: ServerWebExchange, message: String): Mono<Void> {
        val response = exchange.response
        response.statusCode = HttpStatus.UNAUTHORIZED
        response.headers.contentType = MediaType.APPLICATION_JSON
        
        val errorResponse = ApiResponse.error<Nothing>("UNAUTHORIZED", message)
        val responseJson = objectMapper.writeValueAsString(errorResponse)
        
        val dataBuffer = response.bufferFactory().wrap(responseJson.toByteArray())
        return response.writeWith(Mono.just(dataBuffer))
    }
}
