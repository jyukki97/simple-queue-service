package com.example.sqs.common.constants

/**
 * Redis 키 관련 상수
 */
object RedisKeys {
    // 큐 관련
    const val QUEUE_PREFIX = "queue:"
    const val QUEUE_METADATA_PREFIX = "queue:meta:"
    const val QUEUE_STATS_PREFIX = "queue:stats:"
    
    // 메시지 관련
    const val MESSAGE_PREFIX = "message:"
    const val MESSAGE_DETAILS_PREFIX = "message:details:"
    const val MESSAGE_VISIBILITY_PREFIX = "message:visibility:"
    const val MESSAGE_DLQ_PREFIX = "message:dlq:"
    
    // 파티션 관련
    const val PARTITION_PREFIX = "partition:"
    
    // 메트릭 관련
    const val METRICS_PREFIX = "metrics:"
    const val METRICS_TIMESERIES_PREFIX = "metrics:ts:"
    
    // 추적 관련
    const val TRACE_PREFIX = "trace:"
    const val TRACE_INDEX_PREFIX = "trace:index:"
    
    // 스토리지 관련
    const val STORAGE_STATS_PREFIX = "storage:stats:"
    
    // 락 관련
    const val LOCK_PREFIX = "lock:"
}

/**
 * HTTP 헤더 상수
 */
object HttpHeaders {
    const val API_KEY = "X-API-Key"
    const val TENANT_ID = "X-Tenant-Id"
    const val REQUEST_ID = "X-Request-Id"
    const val CORRELATION_ID = "X-Correlation-Id"
}

/**
 * 메트릭 이름 상수
 */
object MetricNames {
    // 메시지 관련
    const val MESSAGES_SENT = "sqs.messages.sent.total"
    const val MESSAGES_RECEIVED = "sqs.messages.received.total"
    const val MESSAGES_DELETED = "sqs.messages.deleted.total"
    const val MESSAGES_FAILED = "sqs.messages.failed.total"
    
    // 큐 관련
    const val QUEUE_DEPTH = "sqs.queue.depth"
    const val QUEUE_PROCESSING_TIME = "sqs.queue.processing.time"
    
    // 시스템 관련
    const val SYSTEM_CPU_USAGE = "sqs.system.cpu.usage"
    const val SYSTEM_MEMORY_USAGE = "sqs.system.memory.usage"
    const val REDIS_CONNECTIONS = "sqs.redis.connections"
}

/**
 * 에러 코드 상수
 */
object ErrorCodes {
    // 일반적인 에러
    const val INTERNAL_ERROR = "INTERNAL_ERROR"
    const val BAD_REQUEST = "BAD_REQUEST"
    const val NOT_FOUND = "NOT_FOUND"
    const val UNAUTHORIZED = "UNAUTHORIZED"
    const val FORBIDDEN = "FORBIDDEN"
    const val CONFLICT = "CONFLICT"
    
    // 큐 관련 에러
    const val QUEUE_NOT_FOUND = "QUEUE_NOT_FOUND"
    const val QUEUE_ALREADY_EXISTS = "QUEUE_ALREADY_EXISTS"
    const val QUEUE_OPERATION_FAILED = "QUEUE_OPERATION_FAILED"
    
    // 메시지 관련 에러
    const val MESSAGE_NOT_FOUND = "MESSAGE_NOT_FOUND"
    const val MESSAGE_TOO_LARGE = "MESSAGE_TOO_LARGE"
    const val MESSAGE_PROCESSING_FAILED = "MESSAGE_PROCESSING_FAILED"
    
    // 테넌트 관련 에러
    const val TENANT_NOT_FOUND = "TENANT_NOT_FOUND"
    const val INVALID_TENANT = "INVALID_TENANT"
    
    // 인증/인가 에러
    const val AUTHENTICATION_FAILED = "AUTHENTICATION_FAILED"
    const val AUTHORIZATION_FAILED = "AUTHORIZATION_FAILED"
    const val INVALID_API_KEY = "INVALID_API_KEY"
    
    // 할당량 에러
    const val QUOTA_EXCEEDED = "QUOTA_EXCEEDED"
    const val RATE_LIMIT_EXCEEDED = "RATE_LIMIT_EXCEEDED"
}

/**
 * 스토리지 계층 상수
 */
object StorageTiers {
    const val HOT = "HOT"
    const val WARM = "WARM" 
    const val COLD = "COLD"
}

/**
 * 시간 관련 상수
 */
object TimeConstants {
    const val SECONDS_PER_MINUTE = 60L
    const val MINUTES_PER_HOUR = 60L
    const val HOURS_PER_DAY = 24L
    const val DAYS_PER_WEEK = 7L
    
    const val MILLISECONDS_PER_SECOND = 1000L
    const val MICROSECONDS_PER_MILLISECOND = 1000L
    const val NANOSECONDS_PER_MICROSECOND = 1000L
}

/**
 * 기본값 상수
 */
object Defaults {
    const val DEFAULT_VISIBILITY_TIMEOUT = 30
    const val DEFAULT_MESSAGE_RETENTION_PERIOD = 1209600L // 14일
    const val DEFAULT_MAX_RECEIVE_COUNT = 3
    const val DEFAULT_PARTITION_COUNT = 4
    const val DEFAULT_BATCH_SIZE = 10
    const val DEFAULT_PAGE_SIZE = 20
    const val MAX_PAGE_SIZE = 100
}
