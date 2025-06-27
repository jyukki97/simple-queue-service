package com.example.sqs.storage.service.cold

import com.example.sqs.storage.domain.cold.FileMetadata
import com.example.sqs.storage.domain.cold.SystemMetrics
import kotlinx.coroutines.Dispatchers
import kotlinx.coroutines.runBlocking
import kotlinx.coroutines.withContext
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.scheduling.annotation.Scheduled
import org.springframework.stereotype.Component
import java.nio.file.Files
import java.nio.file.Path
import java.nio.file.Paths
import java.security.MessageDigest
import java.time.Instant
import java.util.concurrent.ConcurrentHashMap

/**
 * 콜드 스토리지 무결성 검증 서비스
 */
@Component  
class ColdStorageIntegrityService(
    private val fileUtils: ColdStorageFileUtils
) {
    private val logger = LoggerFactory.getLogger(ColdStorageIntegrityService::class.java)
    
    @Value("\${app.storage.cold.phase2.integrity-check-cpu-threshold:0.8}")
    private val integrityCheckCpuThreshold: Double = 0.8
    
    // 체크섬 캐시
    private val checksumCache = ConcurrentHashMap<String, String>()
    
    // 무결성 검증 큐
    private val integrityCheckQueue = ConcurrentHashMap<String, Instant>()

    /**
     * 비동기 SHA-256 체크섬 계산
     */
    suspend fun calculateSHA256Async(data: ByteArray): String = withContext(Dispatchers.IO) {
        val digest = MessageDigest.getInstance("SHA-256")
        val hash = digest.digest(data)
        hash.joinToString("") { "%02x".format(it) }
    }

    /**
     * 스마트 무결성 검증 스케줄링
     */
    fun scheduleIntegrityCheck(filePath: Path) {
        val metrics = SystemMetrics.getCurrent()

        // CPU 사용률이 높으면 검증을 지연
        if (metrics.cpuUsage < integrityCheckCpuThreshold) {
            integrityCheckQueue[filePath.toString()] = Instant.now()
        } else {
            // CPU가 높으면 나중에 검증하도록 지연 스케줄링
            integrityCheckQueue[filePath.toString()] = Instant.now().plusSeconds(3600) // 1시간 후
        }
    }

    /**
     * 정기적 무결성 검증 (백그라운드)
     */
    @Scheduled(fixedRate = 300000) // 5분마다
    fun performScheduledIntegrityChecks() {
        if (integrityCheckQueue.isEmpty()) return

        val metrics = SystemMetrics.getCurrent()
        if (metrics.cpuUsage > integrityCheckCpuThreshold) {
            logger.debug("CPU 사용률이 높아 무결성 검증 건너뜀: ${metrics.cpuUsage}")
            return
        }

        runBlocking {
            performBatchIntegrityCheck()
        }
    }

    /**
     * 배치 무결성 검증
     */
    private suspend fun performBatchIntegrityCheck() = withContext(Dispatchers.IO) {
        val now = Instant.now()
        val filesToCheck = integrityCheckQueue.filter { (_, scheduledTime) ->
            now.isAfter(scheduledTime)
        }.keys.take(10) // 한 번에 최대 10개 파일만 검증

        if (filesToCheck.isEmpty()) return@withContext

        val startTime = System.currentTimeMillis()
        var verifiedCount = 0
        var errorCount = 0

        logger.info("배치 무결성 검증 시작: ${filesToCheck.size}개 파일")

        for (filePathStr in filesToCheck) {
            try {
                val filePath = Paths.get(filePathStr)
                if (Files.exists(filePath)) {
                    val isValid = verifyFileIntegrity(filePath)
                    if (isValid) {
                        verifiedCount++
                    } else {
                        errorCount++
                    }
                }
            } catch (e: Exception) {
                errorCount++
                logger.error("파일 무결성 검증 중 오류: $filePathStr", e)
            } finally {
                integrityCheckQueue.remove(filePathStr)
            }
        }

        val totalTime = System.currentTimeMillis() - startTime
        logger.info("배치 무결성 검증 완료: ${filesToCheck.size}개 파일, 성공: $verifiedCount, 실패: $errorCount, 소요시간: ${totalTime}ms")
    }

    /**
     * 개별 파일 무결성 검증
     */
    suspend fun verifyFileIntegrity(filePath: Path): Boolean = withContext(Dispatchers.IO) {
        try {
            // 메타데이터에서 체크섬 조회
            val metadata = fileUtils.readMetadata(filePath.parent, filePath.fileName.toString())
            if (metadata?.checksum == null) {
                logger.debug("체크섬 없는 파일, 검증 건너뜀: $filePath")
                return@withContext true
            }

            // 파일 데이터 읽기 및 체크섬 계산
            val fileData = Files.readAllBytes(filePath)
            val actualChecksum = calculateSHA256Async(fileData)

            val isValid = actualChecksum == metadata.checksum

            if (!isValid) {
                logger.error("체크섬 불일치 감지: $filePath, 예상=${metadata.checksum}, 실제=$actualChecksum")
                handleCorruptedFile(filePath)
            }

            isValid

        } catch (e: Exception) {
            logger.error("파일 무결성 검증 실패: $filePath", e)
            false
        }
    }

    /**
     * 손상된 파일 처리
     */
    fun handleCorruptedFile(filePath: Path) {
        try {
            val backupPath = filePath.parent.resolve("corrupted_${System.currentTimeMillis()}_${filePath.fileName}")
            Files.move(filePath, backupPath)
            logger.error("손상된 파일을 백업으로 이동: $filePath -> $backupPath")
        } catch (e: Exception) {
            logger.error("손상된 파일 처리 실패: $filePath", e)
        }
    }

    /**
     * 체크섬 캐시 업데이트
     */
    fun updateChecksumCache(filePath: String, checksum: String) {
        checksumCache[filePath] = checksum
    }

    /**
     * 무결성 검증 상태 조회
     */
    fun getIntegrityStatus(): Map<String, Any> {
        return mapOf(
            "pending_checks" to integrityCheckQueue.size,
            "cached_checksums" to checksumCache.size,
            "cpu_threshold" to integrityCheckCpuThreshold,
            "current_cpu_usage" to SystemMetrics.getCurrent().cpuUsage
        )
    }
}
