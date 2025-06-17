# Simple Queue Service

AWS SQS와 유사한 경량 메시지 큐 시스템

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
