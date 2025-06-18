package com.example.sqs.auth.domain

/**
 * 인증된 요청의 컨텍스트 정보
 */
data class AuthContext(
    val tenantId: String,
    val apiKey: String,
    val isAdmin: Boolean = false
) {
    companion object {
        private val contextHolder = ThreadLocal<AuthContext?>()
        
        fun setCurrent(context: AuthContext?) {
            contextHolder.set(context)
        }
        
        fun getCurrent(): AuthContext? {
            return contextHolder.get()
        }
        
        fun getCurrentTenantId(): String {
            return getCurrent()?.tenantId 
                ?: throw IllegalStateException("No authenticated tenant found")
        }
        
        fun clear() {
            contextHolder.remove()
        }
    }
}
