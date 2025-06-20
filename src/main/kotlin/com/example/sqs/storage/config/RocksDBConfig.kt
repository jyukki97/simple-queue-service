package com.example.sqs.storage.config

import org.rocksdb.*
import org.slf4j.LoggerFactory
import org.springframework.beans.factory.annotation.Value
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Profile
import java.nio.file.Files
import java.nio.file.Paths
import jakarta.annotation.PreDestroy

/**
 * RocksDB 설정 및 관리 클래스
 * WARM 계층에서 메시지 저장용으로 사용
 */
@Configuration
@Profile("!test") // 테스트 환경에서는 제외
class RocksDBConfig {

    private val logger = LoggerFactory.getLogger(RocksDBConfig::class.java)

    @Value("\${storage.warm.rocksdb.path:./data/warm}")
    private lateinit var dbPath: String

    @Value("\${storage.warm.rocksdb.max-open-files:1000}")
    private var maxOpenFiles: Int = 1000

    @Value("\${storage.warm.rocksdb.write-buffer-size:67108864}") // 64MB
    private var writeBufferSize: Long = 67108864

    @Value("\${storage.warm.rocksdb.max-write-buffer-number:3}")
    private var maxWriteBufferNumber: Int = 3

    @Value("\${storage.warm.rocksdb.level0-file-num-compaction-trigger:4}")
    private var level0FileNumCompactionTrigger: Int = 4

    @Value("\${storage.warm.rocksdb.target-file-size-base:67108864}") // 64MB
    private var targetFileSizeBase: Long = 67108864

    @Value("\${storage.warm.rocksdb.max-bytes-for-level-base:268435456}") // 256MB
    private var maxBytesForLevelBase: Long = 268435456

    private var rocksDB: RocksDB? = null
    private var options: Options? = null

    init {
        RocksDB.loadLibrary()
    }

    @Bean
    fun rocksDB(): RocksDB {
        return try {
            // 디렉토리 생성
            val path = Paths.get(dbPath)
            if (!Files.exists(path)) {
                Files.createDirectories(path)
                logger.info("RocksDB 디렉토리 생성: $dbPath")
            }

            // Options 설정
            options = createRocksDBOptions()
            
            // RocksDB 열기
            rocksDB = RocksDB.open(options, dbPath)
            logger.info("RocksDB 초기화 완료: $dbPath")
            
            rocksDB!!
        } catch (e: Exception) {
            logger.error("RocksDB 초기화 실패", e)
            throw RuntimeException("RocksDB 초기화 실패", e)
        }
    }

    /**
     * RocksDB 옵션 설정
     * 성능 최적화를 위한 튜닝 포함
     */
    private fun createRocksDBOptions(): Options {
        return Options().apply {
            // 기본 설정
            setCreateIfMissing(true)
            setMaxOpenFiles(maxOpenFiles)
            
            // Write Buffer 설정 (메모리 사용량 제어)
            setWriteBufferSize(writeBufferSize)
            setMaxWriteBufferNumber(maxWriteBufferNumber)
            setMinWriteBufferNumberToMerge(1)
            
            // Compaction 설정 (성능 최적화)
            setLevel0FileNumCompactionTrigger(level0FileNumCompactionTrigger)
            setLevel0SlowdownWritesTrigger(20)
            setLevel0StopWritesTrigger(36)
            setTargetFileSizeBase(targetFileSizeBase)
            setTargetFileSizeMultiplier(1)
            setMaxBytesForLevelBase(maxBytesForLevelBase)
            setMaxBytesForLevelMultiplier(10.0)
            
            // Bloom Filter 설정 (읽기 성능 향상)
            val tableConfig = BlockBasedTableConfig()
            tableConfig.setFilterPolicy(BloomFilter(10.0, false))
            tableConfig.setBlockCacheSize(32 * 1024 * 1024) // 32MB
            tableConfig.setCacheIndexAndFilterBlocks(true)
            setTableFormatConfig(tableConfig)
            
            // 압축 설정
            setCompressionType(CompressionType.LZ4_COMPRESSION)
            setCompressionPerLevel(listOf(
                CompressionType.NO_COMPRESSION,
                CompressionType.NO_COMPRESSION,
                CompressionType.LZ4_COMPRESSION,
                CompressionType.LZ4_COMPRESSION,
                CompressionType.LZ4_COMPRESSION,
                CompressionType.ZSTD_COMPRESSION,
                CompressionType.ZSTD_COMPRESSION
            ))
            
            // WAL 설정
            setMaxTotalWalSize(128 * 1024 * 1024) // 128MB
            
            // 통계 수집 활성화
            setStatistics(Statistics())
            
            logger.info("RocksDB 옵션 설정 완료 - WriteBuffer: ${writeBufferSize / 1024 / 1024}MB, MaxOpenFiles: $maxOpenFiles")
        }
    }

    /**
     * RocksDB 통계 정보 조회
     */
    fun getStatistics(): Map<String, String> {
        return try {
            val stats = options?.statistics()
            if (stats != null) {
                mapOf(
                    "total_keys" to stats.getTickerCount(TickerType.NUMBER_KEYS_WRITTEN).toString(),
                    "total_reads" to stats.getTickerCount(TickerType.NUMBER_DB_SEEK).toString(),
                    "total_writes" to stats.getTickerCount(TickerType.NUMBER_DB_NEXT).toString(),
                    "compaction_count" to stats.getTickerCount(TickerType.COMPACT_READ_BYTES).toString(),
                    "memory_usage" to stats.getTickerCount(TickerType.MEMTABLE_HIT).toString()
                )
            } else {
                emptyMap()
            }
        } catch (e: Exception) {
            logger.warn("RocksDB 통계 조회 실패", e)
            emptyMap()
        }
    }

    /**
     * 수동 컴팩션 실행
     */
    fun compactRange() {
        try {
            rocksDB?.compactRange()
            logger.info("RocksDB 수동 컴팩션 완료")
        } catch (e: Exception) {
            logger.error("RocksDB 컴팩션 실패", e)
        }
    }

    /**
     * 애플리케이션 종료 시 RocksDB 정리
     */
    @PreDestroy
    fun cleanup() {
        try {
            rocksDB?.close()
            options?.close()
            logger.info("RocksDB 리소스 정리 완료")
        } catch (e: Exception) {
            logger.error("RocksDB 정리 중 오류", e)
        }
    }
}
