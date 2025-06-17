package com.example.sqs.performance.compression

import org.springframework.stereotype.Component
import java.io.ByteArrayInputStream
import java.io.ByteArrayOutputStream
import java.nio.charset.StandardCharsets
import java.util.zip.GZIPInputStream
import java.util.zip.GZIPOutputStream

/**
 * 메시지 압축/해제 서비스
 */
@Component
class MessageCompressionService {
    
    /**
     * 메시지를 GZIP으로 압축
     */
    fun compress(message: String): ByteArray {
        val byteArrayOutputStream = ByteArrayOutputStream()
        GZIPOutputStream(byteArrayOutputStream).use { gzipOutputStream ->
            gzipOutputStream.write(message.toByteArray(StandardCharsets.UTF_8))
        }
        return byteArrayOutputStream.toByteArray()
    }
    
    /**
     * GZIP 압축된 메시지를 해제
     */
    fun decompress(compressedData: ByteArray): String {
        val byteArrayInputStream = ByteArrayInputStream(compressedData)
        GZIPInputStream(byteArrayInputStream).use { gzipInputStream ->
            return gzipInputStream.readBytes().toString(StandardCharsets.UTF_8)
        }
    }
    
    /**
     * 압축이 필요한지 확인 (threshold 기준)
     */
    fun shouldCompress(message: String, threshold: Int): Boolean {
        return message.toByteArray(StandardCharsets.UTF_8).size >= threshold
    }
    
    /**
     * 압축률 계산
     */
    fun calculateCompressionRatio(originalSize: Int, compressedSize: Int): Double {
        return if (originalSize > 0) {
            (originalSize - compressedSize).toDouble() / originalSize * 100
        } else {
            0.0
        }
    }
}
