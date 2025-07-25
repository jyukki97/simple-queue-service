# Backend Documentation - Simple Queue Service

## Overview

The Simple Queue Service backend is built with **Kotlin + Spring Boot WebFlux** (reactive) and implements a **3-tier storage architecture** with **multi-tenant support**. The system provides SQS-like queue functionality with advanced features like intelligent tiering, batch processing, and comprehensive monitoring.

## Architecture

### Core Principles

- **Reactive Programming**: Uses Spring WebFlux throughout - all services return `Mono<>` or `Flux<>` types
- **Multi-Tenant Architecture**: Every operation is tenant-scoped using API key authentication
- **Domain-Driven Design**: Clear separation of domains with their own repositories, services, and controllers
- **3-Tier Storage System**: Hot (Redis) → Warm (RocksDB) → Cold (File System)

### Technology Stack

- **Language**: Kotlin 1.9.24
- **Runtime**: Java 21
- **Framework**: Spring Boot 3.2.6 with WebFlux
- **Storage**: Redis (Hot), RocksDB (Warm), File System (Cold)
- **Build**: Gradle with Kotlin DSL
- **Testing**: JUnit 5, Embedded Redis

## Domain Architecture

### 1. Auth Domain (`auth/`)
- **Purpose**: API key-based authentication and authorization
- **Key Components**:
  - `ApiKeyAuthenticationFilter.kt`: Validates API keys for all requests
  - `AuthContext.kt`: Thread-local context for tenant identification
- **API Keys**:
  - Admin: `"admin-api-key"`
  - Test Tenant: `"test-api-key"`

### 2. Tenant Domain (`tenant/`)
- **Purpose**: Multi-tenant isolation and management
- **Key Components**:
  - `TenantService.kt`: Tenant lifecycle management
  - `InMemoryTenantRepository.kt`: In-memory tenant storage (not persistent)
- **Features**: Tenant creation, suspension, API key rotation

### 3. Queue Domain (`queue/`)
- **Purpose**: Queue management and configuration
- **Key Components**:
  - `QueueService.kt`: Core queue operations
  - `QueueController.kt`: REST endpoints for queue management
- **Queue Types**: FIFO and Standard queues
- **Features**: Visibility timeout, message retention, delay seconds, DLQ support

### 4. Message Domain (`message/`)
- **Purpose**: Message lifecycle management
- **Key Components**:
  - `MessageService.kt`: Core message operations
  - `MessageBatchService.kt`: Batch operations
  - `IntelligentBatchService.kt`: Smart batch processing
  - `MessageFilterService.kt`: Advanced message filtering
- **Features**: Message validation, compression, batch operations

### 5. Storage Domain (`storage/`)
- **Purpose**: 3-tier storage system coordination
- **Key Components**:
  - `UnifiedStorageService.kt`: **Main entry point** for all storage operations
  - `TieringManagerService.kt`: Handles automatic data movement between tiers
  - Storage implementations: `HotStorageService.kt`, `WarmStorageService.kt`, `ColdStorageService.kt`
- **Tiering Policies**: Different policies for FIFO vs Standard queues

### 6. Admin Domain (`admin/`)
- **Purpose**: Dashboard APIs and system management
- **Key Components**:
  - `AdminDashboardService.kt`: System statistics and monitoring
  - `TenantManagementService.kt`: Tenant administration
- **Features**: Activity logging, system alerts, usage tracking

## Storage Architecture

### Hot Storage (Redis)
- **Access Time**: < 1ms
- **TTL**: 1 hour
- **Use Case**: Active messages, high-frequency access
- **Configuration**: Lettuce connection pool, reactive operations

### Warm Storage (RocksDB)
- **Access Time**: 1-10ms
- **TTL**: 24 hours
- **Use Case**: Intermediate frequency access
- **Configuration**: Optimized for read/write performance
  - Write buffer: 64MB
  - Max open files: 1000
  - Compression: Enabled

### Cold Storage (File System)
- **Access Time**: 100-1000ms
- **TTL**: 14 days
- **Use Case**: Long-term archival
- **Features**: Compression, integrity validation, batch processing

### Tiering Logic

```yaml
Hot → Warm: After 1 hour OR when Hot storage reaches capacity
Warm → Cold: After 24 hours OR when Warm storage reaches capacity
Access Pattern: Recent access promotes messages back to higher tiers
```

## Configuration

### Application Configuration (`application.yml`)

```yaml
app:
  queue:
    default-visibility-timeout: 30
    max-message-size: 262144  # 256KB
    max-messages-per-batch: 10
    default-message-retention-period: 1209600  # 14 days
  
  storage:
    hot:
      ttl-hours: 1
    warm:
      ttl-days: 1
      rocksdb:
        path: ./data/warm
        write-buffer-size: 67108864  # 64MB
    cold:
      base-path: ./data/cold
      compression-enabled: true
```

### Redis Configuration
- **Connection Pool**: Max 20 connections, min 2 idle
- **Timeout**: 2 seconds
- **Pipeline**: Enabled for batch operations

### Performance Optimizations
- **Compression**: Messages > 1KB are compressed
- **Partitioning**: Queues can be partitioned for high throughput
- **Batch Processing**: Up to 10 messages per batch operation

## API Endpoints

### Queue Management
- `POST /api/queues` - Create queue
- `GET /api/queues` - List queues
- `DELETE /api/queues/{queueName}` - Delete queue
- `POST /api/queues/{queueName}/purge` - Purge all messages

### Message Operations
- `POST /api/queues/{queueName}/messages` - Send message
- `POST /api/queues/{queueName}/messages/batch` - Send batch messages
- `GET /api/queues/{queueName}/messages` - Receive messages
- `DELETE /api/queues/{queueName}/messages` - Delete message
- `POST /api/queues/{queueName}/messages/batch-delete` - Delete batch messages

### Storage Management
- `GET /api/storage/stats` - Storage tier statistics
- `POST /api/storage/tier/{messageId}` - Manual tier promotion
- `GET /api/storage/cold/messages` - List cold storage messages

### Admin & Monitoring
- `GET /api/admin/dashboard` - System overview
- `GET /api/admin/tenants` - Tenant management
- `GET /actuator/health` - Health check
- `GET /actuator/metrics` - System metrics
- `GET /actuator/prometheus` - Prometheus metrics

## Testing

### Unit Tests
```bash
./gradlew test
```

### Performance Tests
```bash
./gradlew performanceTest
```

### Test Configuration
- **Embedded Redis**: Uses `com.github.kstyrc:embedded-redis`
- **Parallel Execution**: Enabled for faster test runs
- **Coverage**: JaCoCo integration available

## Monitoring & Observability

### Health Checks
- Redis connectivity
- Storage tier health
- Queue service status

### Metrics
- Message throughput
- Storage tier statistics
- Queue depth monitoring
- Tenant usage metrics

### Logging
- **Level**: DEBUG for com.example.sqs
- **Pattern**: Includes timestamp, thread, level, logger, and message
- **Tracing**: Full message lifecycle tracking via `MessageTraceService.kt`

## Development Setup

### Prerequisites
- Java 21
- Redis server
- Docker (optional)

### Local Development
1. Start Redis: `docker-compose -f docker-compose.dev.yml up -d`
2. Create storage directories: `mkdir -p ./data/warm ./data/cold`
3. Run application: `./gradlew bootRun`
4. Access health check: `http://localhost:8080/actuator/health`

### Build & Deploy
```bash
# Clean build
./gradlew clean build

# Docker build
docker-compose up -d
```

## Security

### Authentication
- API key-based authentication only
- No user/password system
- Admin operations require admin API key

### Tenant Isolation
- Complete isolation between tenants
- Redis key prefixing with tenant ID
- No cross-tenant data access

### CORS
- Configured for frontend development
- Production settings should be more restrictive

## Performance Characteristics

### Throughput
- **Hot Storage**: 10,000+ ops/sec
- **Warm Storage**: 1,000+ ops/sec
- **Cold Storage**: 100+ ops/sec

### Scalability
- Horizontal scaling via Redis clustering
- Queue partitioning for high-throughput scenarios
- Configurable connection pools and thread pools

### Memory Management
- Automatic tiering prevents memory exhaustion
- Configurable TTL for each storage tier
- Compression reduces memory footprint