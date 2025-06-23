package com.example.sqs

import org.springframework.boot.autoconfigure.SpringBootApplication
import org.springframework.boot.runApplication
import org.springframework.scheduling.annotation.EnableScheduling

/**
 * Simple Queue Service 메인 애플리케이션 클래스
 * 
 * 주요 기능:
 * - 분산 메시지 큐 서비스
 * - 멀티 테넌트 지원
 * - Hot-Warm-Cold 스토리지 계층화
 * - 실시간 메트릭 및 모니터링
 * - 관리자 대시보드
 */
@SpringBootApplication
@EnableScheduling
class SimpleQueueServiceApplication

fun main(args: Array<String>) {
    runApplication<SimpleQueueServiceApplication>(*args)
}
