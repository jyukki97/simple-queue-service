package com.example.sqs.performance.compression

import org.junit.jupiter.api.Test
import org.junit.jupiter.api.assertThrows
import org.springframework.boot.test.context.SpringBootTest
import org.springframework.test.context.ActiveProfiles
import org.springframework.test.context.TestPropertySource
import kotlin.test.assertEquals
import kotlin.test.assertFalse
import kotlin.test.assertTrue

@SpringBootTest
@ActiveProfiles("test")
@TestPropertySource(properties = [
    "app.metrics.enable-prometheus=false"
])
class MessageCompressionServiceTest {
    
    private val compressionService = MessageCompressionService()
    
    @Test
    fun `작은 메시지는 압축하지 않아야 한다`() {
        val smallMessage = "Hello"
        val threshold = 1024
        
        assertFalse(compressionService.shouldCompress(smallMessage, threshold))
    }
    
    @Test
    fun `큰 메시지는 압축해야 한다`() {
        val largeMessage = "x".repeat(2000)
        val threshold = 1024
        
        assertTrue(compressionService.shouldCompress(largeMessage, threshold))
    }
    
    @Test
    fun `메시지 압축과 해제가 올바르게 작동해야 한다`() {
        val originalMessage = "Hello, World! ".repeat(100)
        
        // 압축
        val compressed = compressionService.compress(originalMessage)
        
        // 압축된 데이터가 원본보다 작아야 함
        assertTrue(compressed.size < originalMessage.toByteArray().size)
        
        // 해제
        val decompressed = compressionService.decompress(compressed)
        
        // 해제된 메시지가 원본과 같아야 함
        assertEquals(originalMessage, decompressed)
    }
    
    @Test
    fun `한글 메시지 압축과 해제가 올바르게 작동해야 한다`() {
        val koreanMessage = "안녕하세요! 이것은 한글 메시지입니다. ".repeat(50)
        
        val compressed = compressionService.compress(koreanMessage)
        val decompressed = compressionService.decompress(compressed)
        
        assertEquals(koreanMessage, decompressed)
    }
    
    @Test
    fun `빈 메시지 압축이 올바르게 작동해야 한다`() {
        val emptyMessage = ""
        
        val compressed = compressionService.compress(emptyMessage)
        val decompressed = compressionService.decompress(compressed)
        
        assertEquals(emptyMessage, decompressed)
    }
    
    @Test
    fun `압축률 계산이 올바르게 작동해야 한다`() {
        val originalSize = 1000
        val compressedSize = 300
        
        val ratio = compressionService.calculateCompressionRatio(originalSize, compressedSize)
        
        assertEquals(70.0, ratio, 0.01) // 70% 압축률
    }
    
    @Test
    fun `원본 크기가 0일 때 압축률이 0이어야 한다`() {
        val ratio = compressionService.calculateCompressionRatio(0, 100)
        assertEquals(0.0, ratio)
    }
    
    @Test
    fun `압축된 크기가 원본보다 클 때 음수 압축률을 반환해야 한다`() {
        val originalSize = 100
        val compressedSize = 150
        
        val ratio = compressionService.calculateCompressionRatio(originalSize, compressedSize)
        
        assertEquals(-50.0, ratio, 0.01) // -50% (압축 효과 없음)
    }
    
    @Test
    fun `잘못된 압축 데이터 해제 시 예외가 발생해야 한다`() {
        val invalidData = "invalid data".toByteArray()
        
        assertThrows<Exception> {
            compressionService.decompress(invalidData)
        }
    }
    
    @Test
    fun `JSON 형태의 큰 메시지 압축 테스트`() {
        val jsonMessage = """
            {
                "users": [
                    {"id": 1, "name": "John Doe", "email": "john@example.com", "age": 30},
                    {"id": 2, "name": "Jane Smith", "email": "jane@example.com", "age": 25},
                    {"id": 3, "name": "Bob Johnson", "email": "bob@example.com", "age": 35}
                ],
                "metadata": {
                    "timestamp": "2025-06-17T10:30:00Z",
                    "version": "1.0.0",
                    "source": "user-service"
                }
            }
        """.trimIndent().repeat(20)
        
        val compressed = compressionService.compress(jsonMessage)
        val decompressed = compressionService.decompress(compressed)
        
        assertEquals(jsonMessage, decompressed)
        
        val compressionRatio = compressionService.calculateCompressionRatio(
            jsonMessage.length, compressed.size
        )
        
        // JSON은 압축률이 좋아야 함
        assertTrue(compressionRatio > 50.0, "JSON 압축률이 50% 이상이어야 합니다. 실제: $compressionRatio%")
    }
}
