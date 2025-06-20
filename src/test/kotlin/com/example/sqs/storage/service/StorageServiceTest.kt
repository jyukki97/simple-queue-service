package com.example.sqs.storage.service

import com.example.sqs.message.domain.Message
import com.example.sqs.storage.domain.StorageTier
import com.example.sqs.storage.domain.BatchResult
import com.example.sqs.storage.domain.StorageUsageInfo
import com.example.sqs.storage.service.impl.*
import com.fasterxml.jackson.databind.ObjectMapper
import kotlinx.coroutines.runBlocking
import org.junit.jupiter.api.Test
import org.junit.jupiter.api.extension.ExtendWith
import org.mockito.Mock
import org.mockito.junit.jupiter.MockitoExtension
import org.springframework.data.redis.core.ReactiveRedisTemplate
import java.time.Instant
import kotlin.test.assertEquals
import kotlin.test.assertNotNull
import kotlin.test.assertTrue

@ExtendWith(MockitoExtension::class)
class StorageServiceTest {

    @Mock
    private lateinit var redisTemplate: ReactiveRedisTemplate<String, String>
    
    @Mock
    private lateinit var objectMapper: ObjectMapper
    
    @Test
    fun `핫 스토리지 서비스 생성 테스트`() {
        val hotStorage = HotStorageService(redisTemplate, objectMapper)
        
        assertEquals(StorageTier.HOT, hotStorage.tier)
    }
    
    @Test
    fun `웜 스토리지 서비스 생성 테스트`() {
        val warmStorage = WarmStorageService(redisTemplate, objectMapper)
        
        assertEquals(StorageTier.WARM, warmStorage.tier)
    }
    
    @Test
    fun `콜드 스토리지 서비스 생성 테스트`() {
        val coldStorage = ColdStorageService(objectMapper, "/tmp/test-cold-storage")
        
        assertEquals(StorageTier.COLD, coldStorage.tier)
    }
    
    @Test
    fun `메시지 도메인 생성 테스트`() {
        val message = Message(
            messageId = "test-message-1",
            tenantId = "test-tenant",
            queueName = "test-queue",
            messageBody = "Test message body",
            sentTimestamp = Instant.now(),
            visibleAt = Instant.now()
        )
        
        assertNotNull(message)
        assertEquals("test-message-1", message.messageId)
        assertEquals("test-tenant", message.tenantId)
        assertEquals("test-queue", message.queueName)
    }
    
    @Test
    fun `통합 스토리지 서비스 생성 테스트`() = runBlocking {
        val hotStorage = HotStorageService(redisTemplate, objectMapper)
        val warmStorage = WarmStorageService(redisTemplate, objectMapper)
        val coldStorage = ColdStorageService(objectMapper, "/tmp/test-cold-storage")
        val tieringManager = TieringManagerService(
            hotStorage, warmStorage, coldStorage, redisTemplate, objectMapper, 4
        )
        
        val unifiedStorage = UnifiedStorageService(
            hotStorage, warmStorage, coldStorage, tieringManager
        )
        
        assertNotNull(unifiedStorage)
    }
    
    @Test
    fun `배치 결과 생성 테스트`() {
        val result = BatchResult(
            successful = 5,
            failed = 2,
            errors = listOf("Error 1", "Error 2")
        )
        
        assertEquals(5, result.successful)
        assertEquals(2, result.failed)
        assertEquals(2, result.errors.size)
    }
    
    @Test
    fun `스토리지 사용량 정보 생성 테스트`() {
        val usageInfo = StorageUsageInfo(
            tier = StorageTier.HOT,
            totalCapacityBytes = 1000000,
            usedBytes = 500000,
            availableBytes = 500000,
            usagePercent = 50.0,
            messageCount = 100
        )
        
        assertEquals(StorageTier.HOT, usageInfo.tier)
        assertEquals(50.0, usageInfo.usagePercent)
        assertTrue(usageInfo.messageCount > 0)
    }
}
