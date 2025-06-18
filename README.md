# Simple Queue Service

경량 메시지 큐 시스템

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
- **백엔드 API**: http://localhost:8080
- **관리자 대시보드**: http://localhost:3000
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
│   ├── exception/                     # 예외 처리
│   │   ├── Exceptions.kt              # 커스텀 예외 클래스
│   │   └── GlobalExceptionHandler.kt  # 글로벌 예외 핸들러
│   └── controller/
│       └── HealthController.kt        # 헬스체크 API
├── auth/                              # 인증 및 인가
│   ├── domain/                        # 인증 도메인 모델
│   ├── service/                       # 인증 서비스
│   └── filter/                        # API 키 필터
├── tenant/                            # 테넌트 도메인
│   ├── domain/
│   │   └── Tenant.kt                  # 테넌트 엔티티
│   ├── service/                       # 테넌트 비즈니스 로직
│   └── repository/                    # 테넌트 데이터 액세스
├── queue/                             # 큐 도메인
│   ├── domain/
│   │   ├── Queue.kt                   # 큐 엔티티 & DTO (FIFO 지원)
│   │   └── QueueStatistics.kt         # 큐 통계
│   ├── service/
│   │   └── QueueService.kt            # 큐 비즈니스 로직
│   ├── repository/
│   │   ├── QueueRepository.kt         # 큐 인터페이스
│   │   └── RedisQueueRepository.kt    # Redis 큐 구현
│   └── controller/
│       └── QueueController.kt         # 큐 REST API
├── message/                           # 메시지 도메인
│   ├── domain/
│   │   └── Message.kt                 # 메시지 엔티티 & DTO
│   ├── service/
│   │   └── MessageService.kt          # 메시지 비즈니스 로직
│   ├── repository/
│   │   ├── MessageRepository.kt       # 메시지 인터페이스
│   │   └── RedisMessageRepository.kt  # Redis 메시지 구현
│   └── controller/
│       └── MessageController.kt       # 메시지 REST API
├── admin/                             # 관리자 기능
│   ├── domain/                        # 관리자 도메인 모델
│   ├── service/                       # 대시보드, 테넌트 관리
│   └── controller/                    # 관리자 API
├── metrics/                           # 메트릭 및 모니터링
│   ├── domain/                        # 메트릭 도메인 모델
│   ├── service/                       # 메트릭 수집 및 알림
│   └── controller/                    # 메트릭 API
├── storage/                           # 핫/콜드 스토리지 계층화
│   ├── domain/
│   │   └── StorageTypes.kt            # 스토리지 계층 도메인
│   ├── service/
│   │   ├── StorageInterfaces.kt       # 스토리지 서비스 인터페이스
│   │   ├── UnifiedStorageService.kt   # 통합 스토리지 서비스
│   │   └── impl/                      # 계층별 스토리지 구현
│   │       ├── HotStorageService.kt   # 핫 스토리지 (Redis)
│   │       ├── WarmStorageService.kt  # 웜 스토리지 (Redis Disk)
│   │       ├── ColdStorageService.kt  # 콜드 스토리지 (File)
│   │       └── TieringManagerServiceImpl.kt # 자동 계층화 관리
│   └── controller/
│       └── StorageManagementController.kt # 스토리지 관리 API
├── tracing/                           # 메시지 추적
│   ├── domain/                        # 추적 도메인 모델
│   ├── service/                       # 메시지 추적 서비스
│   └── controller/                    # 추적 API
└── performance/                       # 성능 최적화
    ├── compression/
    │   └── MessageCompressionService.kt  # 메시지 압축
    ├── monitoring/
    │   └── PerformanceMonitoringService.kt # 성능 모니터링
    ├── partitioning/
    │   └── QueuePartitioningService.kt # 큐 파티셔닝
    ├── batch/
    │   └── RedisBatchService.kt        # 배치 처리
    └── cache/
        └── QueueCacheService.kt        # 캐시 최적화

frontend/                              # 관리자 대시보드 (React)
├── src/
│   ├── components/                    # React 컴포넌트
│   ├── services/
│   │   └── adminService.ts            # Admin API 연동
│   └── types/
│       └── admin.ts                   # 타입 정의
└── package.json                       # 프론트엔드 의존성
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

## 🎨 관리자 대시보드

### 프론트엔드 실행

1. 프론트엔드 디렉토리로 이동
```bash
cd frontend
```

2. 의존성 설치 (최초 1회)
```bash
npm install
```

3. 개발 서버 실행
```bash
npm start
```

관리자 대시보드는 http://localhost:3000 에서 접근할 수 있습니다.

### 주요 화면

- **📊 대시보드**: 실시간 시스템 상태 및 메트릭 모니터링
- **📦 큐 관리**: 큐 생성, 수정, 삭제 및 상태 조회
- **👥 테넌트 관리**: 테넌트 생성 및 사용량 관리
- **📈 시스템 모니터링**: 실시간 성능 메트릭 및 알림
