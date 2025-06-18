# Simple Queue Service

AWS SQS와 유사한 경량 메시지 큐 시스템

## 🌟 주요 기능

- **멀티 테넌트 지원**: 완전히 격리된 테넌트별 큐 시스템
- **핫/콜드 스토리지**: 3-tier 자동 계층화 (핫/웜/콜드)
- **FIFO & Standard 큐**: 순서 보장 vs 성능 최적화 선택 가능
- **자동 계층화**: 시간/용량 기반 자동 메시지 이관
- **스마트 승격**: 접근 패턴 기반 지능형 메시지 승격
- **실시간 모니터링**: 계층별 메트릭 및 대시보드
- **REST API**: 완전한 HTTP API 지원

## 🏗️ 스토리지 아키텍처

```
┌─────────────────┐ ┌─────────────────┐ ┌─────────────────┐
│   Hot Storage   │→│  Warm Storage   │→│  Cold Storage   │
│ (Redis Memory)  │ │ (Redis Disk)    │ │ (File System)   │
│   < 1ms access  │ │ 1-10ms access   │ │100-1000ms acc.  │
│   1 hour TTL    │ │  24 hour TTL    │ │  14 days TTL    │
└─────────────────┘ └─────────────────┘ └─────────────────┘
```

## 시작하기

### 필요 조건
- Java 21+
- Docker & Docker Compose

### 개발 환경 실행

1. Redis 실행
```bash
docker-compose -f docker-compose.dev.yml up -d
```

2. 애플리케이션 실행
```bash
./gradlew bootRun
```

### 전체 시스템 실행 (Docker)

```bash
docker-compose up -d
```

## API 테스트

애플리케이션이 실행되면 다음 URL에서 접근 가능합니다:
- API: http://localhost:8080
- Health Check: http://localhost:8080/actuator/health
- Metrics: http://localhost:8080/actuator/metrics

## 개발

### 프로젝트 구조
```
src/main/kotlin/com/example/sqs/
├── SimpleQueueServiceApplication.kt    # 메인 애플리케이션
├── config/                            # 설정 클래스
│   └── RedisConfig.kt
├── common/                            # 공통 모듈
│   ├── domain/
│   │   └── ApiResponse.kt             # 공통 응답 형식
│   └── controller/
│       └── HealthController.kt        # 헬스체크 API
├── tenant/                            # 테넌트 도메인
│   ├── domain/
│   │   └── Tenant.kt                  # 테넌트 엔티티
│   ├── service/                       # 테넌트 비즈니스 로직
│   └── repository/                    # 테넌트 데이터 액세스
├── queue/                             # 큐 도메인
│   ├── domain/
│   │   └── Queue.kt                   # 큐 엔티티 & DTO
│   ├── service/                       # 큐 비즈니스 로직
│   ├── repository/                    # 큐 데이터 액세스
│   └── controller/                    # 큐 REST API
└── message/                           # 메시지 도메인
    ├── domain/
    │   └── Message.kt                 # 메시지 엔티티 & DTO
    ├── service/                       # 메시지 비즈니스 로직
    ├── repository/                    # 메시지 데이터 액세스
    └── controller/                    # 메시지 REST API
```

### 빌드
```bash
./gradlew build
```

### 테스트
```bash
./gradlew test
```

## 문서
자세한 스펙은 [docs/spec.md](docs/spec.md)를 참고하세요.
