package com.example.sqs.streaming.lifecycle

import com.example.sqs.streaming.service.StreamingService
import org.slf4j.LoggerFactory
import org.springframework.boot.context.event.ApplicationReadyEvent
import org.springframework.context.event.ContextClosedEvent
import org.springframework.context.event.EventListener
import org.springframework.stereotype.Component

@Component
class StreamingLifecycleManager(
    private val streamingService: StreamingService
) {

    private val logger = LoggerFactory.getLogger(StreamingLifecycleManager::class.java)

    @EventListener(ApplicationReadyEvent::class)
    fun onApplicationReady() {
        logger.info("Streaming service lifecycle manager started")
        
        // 하트비트 시작
        streamingService.sendHeartbeat()
            .subscribe(
                { logger.debug("Heartbeat sent") },
                { error -> logger.error("Error sending heartbeat", error) }
            )

        // 비활성 연결 정리 시작
        streamingService.cleanupInactiveConnections()
            .subscribe(
                { logger.debug("Inactive connections cleaned up") },
                { error -> logger.error("Error cleaning up inactive connections", error) }
            )

        logger.info("Streaming service background tasks started")
    }

    @EventListener(ContextClosedEvent::class)
    fun onApplicationShutdown() {
        logger.info("Application shutting down - cleaning up streaming connections")
        
        // 모든 연결 정리는 StreamingService의 스프링 빈 생명주기에서 처리됨
        logger.info("Streaming service shutdown completed")
    }
}