package com.example.sqs.storage.config

import org.rocksdb.RocksDB
import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.context.annotation.Primary
import org.springframework.context.annotation.Profile
import org.mockito.Mockito

/**
 * 테스트 환경용 RocksDB 설정
 * 실제 RocksDB 대신 Mock을 사용하여 파일 시스템 충돌 방지
 */
@Configuration
@Profile("test")
class TestRocksDBConfig {

    /**
     * 테스트 환경에서 RocksDB를 Mock으로 대체
     * 파일 시스템 Lock 문제 및 리소스 충돌 방지
     */
    @Bean
    @Primary
    fun rocksDB(): RocksDB {
        return Mockito.mock(RocksDB::class.java)
    }
}
