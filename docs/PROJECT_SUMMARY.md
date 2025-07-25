# Simple Queue Service - Project Summary

## Project Overview

The Simple Queue Service (SQS) is a comprehensive, production-ready message queue system built with **Kotlin + Spring Boot WebFlux** for the backend and **React + TypeScript** for the admin dashboard. The system implements advanced features including **3-tier storage architecture**, **multi-tenant support**, **intelligent message tiering**, and **real-time monitoring**.

## Key Features

### 🏗️ Architecture
- **Reactive Programming**: Full WebFlux implementation with `Mono<>` and `Flux<>` types
- **Multi-Tenant**: Complete tenant isolation with API key authentication
- **Domain-Driven Design**: Clean separation of concerns across business domains
- **3-Tier Storage**: Hot (Redis) → Warm (RocksDB) → Cold (File System)

### 🚀 Core Functionality
- **Queue Types**: Both FIFO and Standard queues with SQS-compatible APIs
- **Message Operations**: Send, receive, delete with batch support (up to 10 messages)
- **Dead Letter Queues**: Automatic handling of failed message processing
- **Message Attributes**: Custom metadata support with JSON serialization
- **Visibility Timeout**: Configurable message visibility for processing

### 💾 Storage System
- **Hot Storage (Redis)**: < 1ms access, 1-hour TTL, active messages
- **Warm Storage (RocksDB)**: 1-10ms access, 24-hour TTL, intermediate frequency
- **Cold Storage (File System)**: 100-1000ms access, 14-day TTL, archival
- **Intelligent Tiering**: Automatic promotion/demotion based on access patterns

### 🎛️ Admin Dashboard
- **Queue Management**: Create, configure, and monitor queues
- **Message Operations**: Send, receive, search, and manage messages
- **Storage Monitoring**: Real-time tier statistics and performance metrics
- **Tenant Administration**: Multi-tenant management and usage tracking
- **System Monitoring**: Health checks, alerts, and performance dashboards

## Technology Stack

### Backend
- **Language**: Kotlin 1.9.24
- **Runtime**: Java 21
- **Framework**: Spring Boot 3.2.6 with WebFlux (Reactive)
- **Storage**: Redis (Hot), RocksDB (Warm), File System (Cold)
- **Build**: Gradle with Kotlin DSL
- **Testing**: JUnit 5, Embedded Redis

### Frontend
- **Framework**: React 19.1.0
- **Language**: TypeScript 4.9.5
- **Routing**: React Router DOM 6.29.0
- **HTTP Client**: Axios 1.10.0
- **Charts**: Recharts 2.14.0
- **Build**: React Scripts 5.0.1

### Infrastructure
- **Database**: Redis 7.x for primary storage
- **File Storage**: Local file system for cold tier
- **Container**: Docker with docker-compose
- **Monitoring**: Prometheus metrics, Actuator endpoints

## Project Structure

```
simple-queue-service/
├── src/main/kotlin/com/example/sqs/     # Backend source code
│   ├── auth/                           # Authentication domain
│   ├── tenant/                         # Multi-tenant management
│   ├── queue/                          # Queue operations
│   ├── message/                        # Message lifecycle
│   ├── storage/                        # 3-tier storage system
│   ├── admin/                          # Admin APIs
│   └── common/                         # Shared utilities
├── frontend/src/                       # React frontend
│   ├── components/                     # React components
│   ├── services/                       # API service layer
│   └── types/                          # TypeScript definitions
├── docs/                              # Project documentation
│   ├── backend/                       # Backend documentation
│   ├── frontend/                      # Frontend documentation
│   └── PROJECT_SUMMARY.md            # This file
└── docker-compose.yml                # Container orchestration
```

## Domain Architecture

### 1. Authentication Domain (`auth/`)
- **API Key Authentication**: Secure, stateless authentication
- **Tenant Context**: Thread-local tenant identification
- **Filter Integration**: WebFlux filter chain integration

### 2. Tenant Domain (`tenant/`)
- **Multi-Tenant Isolation**: Complete resource separation
- **API Key Management**: Generation, rotation, validation
- **Usage Tracking**: Per-tenant resource monitoring

### 3. Queue Domain (`queue/`)
- **Queue Types**: FIFO and Standard with different guarantees
- **Configuration**: Visibility timeout, retention, DLQ setup
- **Statistics**: Real-time queue metrics and monitoring

### 4. Message Domain (`message/`)
- **Lifecycle Management**: Send, receive, delete operations
- **Batch Processing**: Optimized bulk operations
- **Validation**: Size limits, format validation
- **Compression**: Automatic compression for large messages

### 5. Storage Domain (`storage/`)
- **Unified Service**: Single entry point for all storage operations
- **Tiering Manager**: Automatic data movement between tiers
- **Performance Optimization**: Caching, compression, partitioning

### 6. Admin Domain (`admin/`)
- **Dashboard APIs**: System overview and statistics
- **Management Operations**: Tenant and system administration
- **Monitoring**: Alerts, activity logging, performance tracking

## Performance Characteristics

### Throughput Metrics
- **Hot Storage**: 10,000+ operations/second
- **Warm Storage**: 1,000+ operations/second  
- **Cold Storage**: 100+ operations/second
- **API Endpoints**: 500+ requests/second per instance

### Scalability Features
- **Horizontal Scaling**: Redis clustering support
- **Queue Partitioning**: High-throughput queue distribution
- **Connection Pooling**: Optimized resource utilization
- **Batch Operations**: Reduced network overhead

### Resource Management
- **Memory**: Automatic tiering prevents exhaustion
- **Disk**: Configurable limits with cleanup policies
- **Network**: Connection pooling and keep-alive
- **CPU**: Non-blocking I/O with reactive streams

## API Overview

### Queue Operations
```bash
POST   /api/queues                    # Create queue
GET    /api/queues                    # List queues
GET    /api/queues/{name}             # Get queue attributes
DELETE /api/queues/{name}             # Delete queue
POST   /api/queues/{name}/purge       # Purge all messages
```

### Message Operations
```bash
POST   /api/queues/{name}/messages              # Send message
POST   /api/queues/{name}/messages/batch        # Send batch
GET    /api/queues/{name}/messages              # Receive messages
DELETE /api/queues/{name}/messages              # Delete message
POST   /api/queues/{name}/messages/batch-delete # Delete batch
```

### Storage Management
```bash
GET    /api/storage/stats                       # Storage statistics
POST   /api/storage/tier/promote                # Manual tier promotion
GET    /api/storage/cold/messages               # Cold storage listing
```

### Administration
```bash
GET    /api/admin/dashboard                     # System overview
GET    /api/admin/tenants                       # Tenant management
GET    /actuator/health                         # Health check
GET    /actuator/metrics                        # System metrics
```

## Configuration

### Application Configuration
```yaml
app:
  queue:
    default-visibility-timeout: 30
    max-message-size: 262144           # 256KB
    max-messages-per-batch: 10
    default-message-retention-period: 1209600  # 14 days
  
  storage:
    hot:
      ttl-hours: 1
    warm:
      ttl-days: 1
      rocksdb:
        path: ./data/warm
        write-buffer-size: 67108864    # 64MB
    cold:
      base-path: ./data/cold
      compression-enabled: true
```

### Redis Configuration
```yaml
spring:
  redis:
    host: localhost
    port: 6379
    lettuce:
      pool:
        max-active: 20
        max-idle: 8
        min-idle: 2
```

## Development Setup

### Prerequisites
- Java 21
- Node.js 16+
- Redis server
- Docker (optional)

### Quick Start
```bash
# 1. Start Redis
docker-compose -f docker-compose.dev.yml up -d

# 2. Create storage directories
mkdir -p ./data/warm ./data/cold

# 3. Start backend
./gradlew bootRun

# 4. Start frontend
cd frontend && npm install && npm start

# 5. Access dashboard
open http://localhost:3000
```

### Full System with Docker
```bash
# Start entire system
docker-compose up -d

# Check health
curl http://localhost:8080/actuator/health
```

## Testing

### Backend Testing
```bash
# Unit tests
./gradlew test

# Performance tests
./gradlew performanceTest

# Integration tests
./gradlew test --tests "*IntegrationTest"
```

### Frontend Testing
```bash
cd frontend

# Unit tests
npm test

# Coverage report
npm test -- --coverage
```

## Security

### Authentication & Authorization
- **API Key Based**: No user/password complexity
- **Tenant Isolation**: Complete resource separation
- **Admin Access**: Separate admin API key for system operations

### Data Security
- **Encryption**: Optional cold storage encryption
- **Validation**: Input validation and sanitization
- **CORS**: Configurable cross-origin policies

## Monitoring & Observability

### Health Monitoring
- **Actuator Endpoints**: Spring Boot health checks
- **Custom Health Indicators**: Storage tier health
- **Dependency Checks**: Redis, RocksDB, file system

### Metrics Collection
- **Prometheus Integration**: Comprehensive metrics export
- **Custom Metrics**: Business-specific measurements
- **Performance Tracking**: Latency, throughput, error rates

### Logging
- **Structured Logging**: JSON format for log aggregation
- **Trace IDs**: Request correlation across services
- **Configurable Levels**: Environment-specific log levels

## Deployment

### Docker Deployment
```yaml
# docker-compose.yml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "8080:8080"
    depends_on:
      - redis
  
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
```

### Production Considerations
- **Environment Variables**: Externalized configuration
- **Health Checks**: Container and load balancer integration
- **Scaling**: Horizontal scaling with shared Redis
- **Backup**: Data persistence and recovery strategies

## Future Enhancements

### Planned Features
- **WebSocket Support**: Real-time dashboard updates
- **Message Routing**: Advanced routing and filtering
- **Cloud Storage**: S3/GCS integration for cold tier
- **Clustering**: Multi-node deployment support

### Performance Improvements
- **Virtual Threads**: Java 21 virtual thread adoption
- **Native Compilation**: GraalVM native image support
- **Advanced Caching**: Multi-level caching strategies
- **Compression**: Advanced compression algorithms

### Operational Features
- **Auto-Scaling**: Dynamic capacity management
- **Circuit Breakers**: Resilience patterns
- **Rate Limiting**: Per-tenant rate controls
- **Audit Logging**: Compliance and security auditing

## Contributing

### Development Workflow
1. **Feature Branch**: Create from main branch
2. **Implementation**: Follow coding standards
3. **Testing**: Unit and integration tests required
4. **Documentation**: Update relevant docs
5. **Pull Request**: Code review process

### Code Standards
- **Kotlin**: Official Kotlin coding conventions
- **TypeScript**: Strict typing enabled
- **Testing**: Minimum 80% code coverage
- **Documentation**: Comprehensive API documentation

## License

This project is licensed under the MIT License - see the LICENSE file for details.

## Support

### Documentation
- **Backend**: [docs/backend/README.md](backend/README.md)
- **Frontend**: [docs/frontend/README.md](frontend/README.md)
- **API Reference**: [docs/backend/API.md](backend/API.md)

### Contact
- **Issues**: GitHub Issues for bug reports
- **Discussions**: GitHub Discussions for questions
- **Security**: security@example.com for security issues

---

*This project demonstrates modern software architecture principles with production-ready features including reactive programming, multi-tenancy, intelligent storage tiering, and comprehensive monitoring capabilities.*