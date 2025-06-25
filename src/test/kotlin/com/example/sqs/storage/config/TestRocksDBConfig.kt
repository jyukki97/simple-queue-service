package com.example.sqs.storage.config

import org.rocksdb.Options
import org.rocksdb.RocksDB
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import java.io.File
import java.nio.file.Files
import java.nio.file.Paths
import jakarta.annotation.PreDestroy

/**
 * 테스트 환경용 RocksDB 설정
 * 실제 RocksDB를 사용하여 WARM 스토리지 테스트 수행
 */
@Configuration
@Profile("test")
class TestRocksDBConfig {

    @Value("\${storage.warm.rocksdb.path:./data/warm-test}")
    private lateinit var dbPath: String

    private var rocksDB: RocksDB? = null
    private var options: Options? = null

    init {
        RocksDB.loadLibrary()
    }

    /**
     * 테스트 환경에서 실제 RocksDB 사용
     * 파일 시스템에 실제 데이터베이스 파일 생성
     */
    @Bean
    @Primary
    fun rocksDB(): RocksDB {
        return try {
            // 테스트용 디렉토리 생성
            val path = Paths.get(dbPath)
            if (!Files.exists(path)) {
                Files.createDirectories(path)
            }

            // 테스트용 옵션 설정 (간단한 설정)
            options = Options().apply {
                setCreateIfMissing(true)
                setWriteBufferSize(16 * 1024 * 1024) // 16MB
                setMaxOpenFiles(100)
            }
            
            // RocksDB 열기
            rocksDB = RocksDB.open(options, dbPath)
            rocksDB!!
        } catch (e: Exception) {
            throw RuntimeException("테스트 RocksDB 초기화 실패", e)
        }
    }

    @PreDestroy
    fun cleanup() {
        try {
            rocksDB?.close()
            options?.close()
            
            // 테스트 후 파일 정리
            val dbDir = File(dbPath)
            if (dbDir.exists()) {
                dbDir.deleteRecursively()
            }
        } catch (e: Exception) {
            // 테스트 정리 중 오류는 무시
        }
    }
}
