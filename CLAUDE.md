# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## Commands

### Backend (Kotlin/Spring Boot)

```bash
# Build project
./gradlew build

# Run application locally
./gradlew bootRun

# Run unit tests
./gradlew test

# Run performance tests specifically
./gradlew performanceTest

# Clean build
./gradlew clean build
```

### Frontend (React/TypeScript)

```bash
# Install dependencies (run from frontend/ directory)
cd frontend && npm install

# Start development server
cd frontend && npm start

# Build for production
cd frontend && npm run build

# Run frontend tests
cd frontend && npm test
```

### Infrastructure

```bash
# Start Redis for development
docker-compose -f docker-compose.dev.yml up -d

# Start full system with Docker
docker-compose up -d

# Create required storage directories
mkdir -p ./data/warm ./data/cold
```

## Architecture Overview

This is a Simple Queue Service (SQS-like) built with **Kotlin + Spring Boot WebFlux** (reactive) for the backend and **React + TypeScript** for the admin dashboard. The system implements a **3-tier storage architecture** and **multi-tenant queue system**.

### Key Architectural Patterns

**Multi-Tenant Architecture**: Every operation is tenant-scoped using API key authentication. Tenants are completely isolated from each other.

**3-Tier Storage System**:
- **Hot Storage** (Redis Memory): < 1ms access, 1 hour TTL, active messages
- **Warm Storage** (RocksDB): 1-10ms access, 24 hour TTL, intermediate frequency
- **Cold Storage** (File System): 100-1000ms access, 14 days TTL, archival

**Reactive Programming**: Uses Spring WebFlux throughout - all services return `Mono<>` or `Flux<>` types.

**Domain-Driven Design**: Clear separation of domains with their own repositories, services, and controllers.

### Core Domains

1. **Auth Domain** (`src/main/kotlin/com/example/sqs/auth/`):
   - API key-based authentication using `ApiKeyAuthenticationFilter.kt`
   - Thread-local `AuthContext.kt` for tenant identification
   - Admin API key: `"admin-api-key"`, Test tenant API key: `"test-api-key"`

2. **Tenant Domain** (`src/main/kotlin/com/example/sqs/tenant/`):
   - Multi-tenant isolation and management
   - In-memory tenant repository (not persistent)
   - Tenant creation, suspension, API key rotation

3. **Queue Domain** (`src/main/kotlin/com/example/sqs/queue/`):
   - FIFO and Standard queue types
   - Queue attributes: visibility timeout, message retention, delay seconds
   - Dead Letter Queue (DLQ) support

4. **Message Domain** (`src/main/kotlin/com/example/sqs/message/`):
   - Message lifecycle management
   - Batch operations support
   - Message validation and compression

5. **Storage Domain** (`src/main/kotlin/com/example/sqs/storage/`):
   - **Critical**: `UnifiedStorageService.kt` coordinates all storage tiers
   - **Tiering Manager**: `TieringManagerService.kt` handles automatic data movement
   - **Storage Implementations**: `HotStorageService.kt`, `WarmStorageService.kt`, `ColdStorageService.kt`
   - **Policies**: Different tiering policies for FIFO vs Standard queues

6. **Admin Domain** (`src/main/kotlin/com/example/sqs/admin/`):
   - Dashboard APIs and tenant management
   - System statistics and monitoring
   - Activity logging and alerts

### Important Implementation Details

**Storage Coordination**: The `UnifiedStorageService` is the main entry point for all message operations. It automatically routes requests to the appropriate storage tier and handles tier promotion/demotion.

**Scheduler Integration**: `TieringScheduler.kt` runs scheduled tasks to move messages between storage tiers based on access patterns and age.

**Redis Key Structure**: Managed by `RedisKeyManager.kt` with tenant-scoped prefixes to ensure isolation.

**Message Tracing**: Full message lifecycle tracking through `MessageTraceService.kt` for debugging and monitoring.

**Performance Optimization**: 
- Batch operations via `MessageBatchService.kt`
- Compression support via `MessageCompressionService.kt`
- Queue partitioning for high-throughput scenarios

### Configuration

**Application Configuration**: `src/main/resources/application.yml`
- Redis connection settings
- Storage paths and TTL values  
- Scheduling intervals

**Java Version**: Requires Java 21 (specified in `build.gradle.kts`)

**Database**: Redis as primary storage, RocksDB for warm tier, file system for cold tier

### Testing Structure

- **Unit Tests**: Standard tests under `src/test/kotlin/`
- **Performance Tests**: Separate task `./gradlew performanceTest` for load testing
- **Embedded Redis**: Uses embedded Redis for testing via `com.github.kstyrc:embedded-redis`

### Frontend Integration

- **Proxy Configuration**: Frontend proxies to backend at `localhost:8080`
- **API Services**: `frontend/src/services/` contains API clients
- **Hardcoded API Keys**: Frontend uses `"test-api-key"` for development

### Development Workflow

1. Start Redis: `docker-compose -f docker-compose.dev.yml up -d`
2. Create storage directories: `mkdir -p ./data/warm ./data/cold`
3. Run backend: `./gradlew bootRun`
4. Run frontend: `cd frontend && npm start`
5. Access admin dashboard: `http://localhost:3000`

### Monitoring and Observability

- **Health Check**: `/actuator/health`
- **Metrics**: `/actuator/metrics` and `/actuator/prometheus`
- **Custom Metrics**: Storage tier statistics and queue performance metrics
- **Tracing**: Message lifecycle tracking for debugging

### Security Notes

- API key authentication only (no user/password system)
- Tenant isolation through API keys
- CORS configured for frontend development
- Admin operations require admin API key