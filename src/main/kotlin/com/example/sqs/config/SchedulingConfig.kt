package com.example.sqs.config

import org.springframework.context.annotation.Bean
import org.springframework.context.annotation.Configuration
import org.springframework.scheduling.TaskScheduler
import org.springframework.scheduling.concurrent.ThreadPoolTaskScheduler

/**
 * 스케줄링 관련 설정
 */
@Configuration
class SchedulingConfig {
    
    @Bean
    fun taskScheduler(): TaskScheduler {
        val scheduler = ThreadPoolTaskScheduler()
        scheduler.setPoolSize(5)
        scheduler.setThreadNamePrefix("sqs-scheduler-")
        scheduler.setDaemon(true)
        scheduler.setWaitForTasksToCompleteOnShutdown(true)
        scheduler.setAwaitTerminationSeconds(30)
        scheduler.initialize()
        return scheduler
    }
}
