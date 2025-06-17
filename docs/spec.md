# Simple Queue Service
AWS SQS와 유사한 경량 메시지 큐 시스템 구현

## 프로젝트 개요
분산 애플리케이션 간 비동기 메시지 통신을 위한 경량 큐 서비스입니다. AWS SQS의 핵심 기능들을 참고하여 구현하되, 사이드 프로젝트에 적합한 수준으로 단순화했습니다.

## 특징
- **멀티 테넌트 지원**: 고객 단위로 완전히 격리된 큐 시스템
- **지연 큐 (Delay Queue)**: 메시지 전송 후 일정 시간 지연 후 수신 가능
- **가시성 타임아웃 (Visibility Timeout)**: 메시지 처리 중 다른 컨슈머의 중복 처리 방지
- **데드레터 큐 (DLQ)**: 처리 실패한 메시지의 자동 이동
- **메시지 보존**: 최대 14일간 메시지 보관
- **REST API**: 표준 HTTP API를 통한 모든 작업 수행
- **모니터링**: 큐별 메트릭 및 상태 조회

## 주요 도메인

### Tenant (테넌트)
- 고객을 나타내는 최상위 격리 단위
- 각 테넌트는 독립된 큐 네임스페이스를 가짐
- API Key 기반 인증

### Queue (큐)
- 메시지를 저장하는 컨테이너
- 테넌트별로 관리되며, 큐 이름은 테넌트 내에서 유니크
- FIFO 보장 (순서 유지)

### Message (메시지)
- 큐를 통해 전달되는 데이터 단위
- 최대 256KB 크기 제한
- 메타데이터와 함께 저장

## 주요 기능

### 큐 관리
- [x] 큐 생성 (CreateQueue)
  - 큐 속성 설정 (VisibilityTimeout, MessageRetentionPeriod, DelaySeconds 등)
  - DLQ 설정
- [x] 큐 삭제 (DeleteQueue)
  - 큐와 모든 메시지 완전 삭제
- [x] 큐 목록 조회 (ListQueues)
  - 테넌트별 큐 목록
  - 페이징 지원 (limit, offset)
  - 큐 이름 패턴 필터링
- [] 큐 속성 조회 (GetQueueAttributes)
- [] 큐 속성 수정 (SetQueueAttributes)

### 메시지 관리
- [] 메시지 전송 (SendMessage)
  - 단일 메시지 전송
  - DelaySeconds 지원
  - 메시지 속성 (attributes) 지원
- [] 메시지 일괄 전송 (SendMessageBatch)
  - 최대 10개 메시지 동시 전송
- [] 메시지 수신 (ReceiveMessage)
  - Long Polling 지원
  - MaxNumberOfMessages (1-10개)
  - VisibilityTimeout 적용
- [] 메시지 삭제 (DeleteMessage)
  - 처리 완료된 메시지 삭제
- [] 메시지 일괄 삭제 (DeleteMessageBatch)

### 모니터링 및 운영
- [] 큐 통계 조회
  - 대기 중인 메시지 수
  - 처리 중인 메시지 수 (invisible)
  - DLQ 메시지 수
- [] 메시지 내용 미리보기 (PeekMessage)
- [] 큐 퍼지 (PurgeQueue) - 모든 메시지 삭제

## 기술 스택

### 백엔드
- **Spring Boot 3.2** (Kotlin)
- **Spring Data Redis**
- **Spring Web**
- **Jackson** (JSON 처리)
- **Kotlin Coroutines** (비동기 처리)

### 저장소
- **Redis 7.0+**
  - 메시지 저장소
  - 큐 메타데이터 저장
  - 분산 락 구현

### 스케줄링
- **Spring Scheduler**
  - DLQ 이동 작업
  - 만료 메시지 정리
  - 메트릭 수집

### 운영 도구
- **Docker Compose**
  - Redis, Application 컨테이너 관리
- **Gradle** (빌드 도구)
- **Micrometer** (메트릭)

## 시스템 아키텍처

```
┌─────────────────┐    ┌─────────────────┐    ┌─────────────────┐
│   Producer      │    │   Queue Service │    │   Consumer      │
│   Application   │───▶│   (Spring Boot) │◀───│   Application   │
└─────────────────┘    └─────────────────┘    └─────────────────┘
                               │
                               ▼
                       ┌─────────────────┐
                       │     Redis       │
                       │  Message Store  │
                       └─────────────────┘
```

## API 명세

### 인증
- **API Key** 기반 인증
- Header: `X-API-Key: {tenant-api-key}`
- 모든 API 요청에 필수

### 공통 응답 형식
```json
{
  "success": true,
  "data": {},
  "error": {
    "code": "ERROR_CODE",
    "message": "에러 메시지"
  },
  "timestamp": "2025-06-17T10:30:00Z"
}
```

### 큐 관리 API

#### 1. 큐 생성
```http
POST /api/v1/queues
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "queueName": "my-queue",
  "attributes": {
    "visibilityTimeoutSeconds": 30,
    "messageRetentionPeriod": 1209600,
    "delaySeconds": 0,
    "maxReceiveCount": 3,
    "deadLetterTargetArn": "my-dlq"
  }
}
```

#### 2. 큐 목록 조회
```http
GET /api/v1/queues?limit=20&offset=0&namePattern=test-*
X-API-Key: {tenant-api-key}
```

#### 3. 큐 속성 조회
```http
GET /api/v1/queues/{queueName}/attributes
X-API-Key: {tenant-api-key}
```

#### 4. 큐 삭제
```http
DELETE /api/v1/queues/{queueName}
X-API-Key: {tenant-api-key}
```

### 메시지 관리 API

#### 1. 메시지 전송
```http
POST /api/v1/queues/{queueName}/messages
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "messageBody": "Hello World",
  "delaySeconds": 0,
  "attributes": {
    "customKey": "customValue"
  }
}
```

#### 2. 메시지 일괄 전송
```http
POST /api/v1/queues/{queueName}/messages/batch
Content-Type: application/json
X-API-Key: {tenant-api-key}

{
  "messages": [
    {
      "id": "msg1",
      "messageBody": "Message 1",
      "delaySeconds": 0
    },
    {
      "id": "msg2", 
      "messageBody": "Message 2",
      "delaySeconds": 5
    }
  ]
}
```

#### 3. 메시지 수신
```http
GET /api/v1/queues/{queueName}/messages?maxNumberOfMessages=10&visibilityTimeoutSeconds=30&waitTimeSeconds=20
X-API-Key: {tenant-api-key}
```

#### 4. 메시지 삭제
```http
DELETE /api/v1/queues/{queueName}/messages/{receiptHandle}
X-API-Key: {tenant-api-key}
```

### 모니터링 API

#### 큐 통계 조회
```http
GET /api/v1/queues/{queueName}/stats
X-API-Key: {tenant-api-key}
```

## 데이터 모델

### Queue Entity
```kotlin
data class Queue(
    val tenantId: String,
    val queueName: String,
    val attributes: QueueAttributes,
    val createdAt: Instant,
    val updatedAt: Instant
)

data class QueueAttributes(
    val visibilityTimeoutSeconds: Int = 30,
    val messageRetentionPeriod: Int = 1209600, // 14일
    val delaySeconds: Int = 0,
    val maxReceiveCount: Int = 3,
    val deadLetterTargetArn: String? = null,
    val redrivePolicy: RedrivePolicy? = null
)
```

### Message Entity  
```kotlin
data class Message(
    val messageId: String,
    val tenantId: String,
    val queueName: String,
    val messageBody: String,
    val attributes: Map<String, String> = emptyMap(),
    val sentTimestamp: Instant,
    val firstReceiveTimestamp: Instant? = null,
    val receiveCount: Int = 0,
    val visibleAt: Instant,
    val receiptHandle: String? = null
)
```

### Tenant Entity
```kotlin
data class Tenant(
    val tenantId: String,
    val apiKey: String,
    val name: String,
    val createdAt: Instant,
    val isActive: Boolean = true
)
```

## Redis 데이터 구조

### 1. 큐 메타데이터
```
Key: queue:{tenantId}:{queueName}
Type: Hash
Fields:
- name: 큐 이름
- visibilityTimeout: 가시성 타임아웃
- messageRetention: 메시지 보존 기간
- delaySeconds: 기본 지연 시간
- maxReceiveCount: 최대 수신 횟수
- deadLetterQueue: DLQ 이름
- createdAt: 생성 시간
```

### 2. 메시지 대기 큐 (Available)
```
Key: queue:messages:{tenantId}:{queueName}
Type: List (FIFO)
Value: messageId
```

### 3. 지연 메시지 (Delayed)
```
Key: queue:delayed:{tenantId}:{queueName}
Type: Sorted Set
Score: 실행 시간 (timestamp)
Value: messageId
```

### 4. 처리 중 메시지 (In-Flight)
```
Key: queue:inflight:{tenantId}:{queueName}
Type: Sorted Set  
Score: 가시성 복원 시간
Value: messageId:receiptHandle
```

### 5. 메시지 상세 정보
```
Key: message:{messageId}
Type: Hash
Fields:
- body: 메시지 내용
- attributes: 메시지 속성 (JSON)
- sentTimestamp: 전송 시간
- receiveCount: 수신 횟수
- firstReceiveTimestamp: 첫 수신 시간
```

### 6. 테넌트별 큐 목록
```
Key: tenant:queues:{tenantId}
Type: Set
Value: queueName
```

## 핵심 알고리즘

### 메시지 전송 (SendMessage)
1. 메시지 ID 생성 (UUID)
2. 메시지 상세 정보 저장
3. DelaySeconds > 0인 경우: delayed queue에 추가
4. DelaySeconds = 0인 경우: available queue에 추가
5. 큐 통계 업데이트

### 메시지 수신 (ReceiveMessage)
1. In-flight 큐에서 만료된 메시지 복원
2. Delayed 큐에서 실행 시간이 된 메시지를 available로 이동
3. Available 큐에서 메시지 poll
4. Receipt handle 생성 및 in-flight 큐에 추가
5. 메시지 수신 횟수 증가
6. MaxReceiveCount 초과시 DLQ로 이동

### 가시성 타임아웃 처리
- Spring Scheduler로 주기적 실행 (30초마다)
- In-flight 큐에서 타임아웃된 메시지 탐지
- Available 큐로 메시지 복원
- Receipt handle 무효화

## 에러 처리

### 에러 코드 정의
```kotlin
enum class ErrorCode(val message: String) {
    QUEUE_NOT_FOUND("큐를 찾을 수 없습니다"),
    QUEUE_ALREADY_EXISTS("큐가 이미 존재합니다"),
    MESSAGE_NOT_FOUND("메시지를 찾을 수 없습니다"),
    INVALID_RECEIPT_HANDLE("잘못된 receipt handle입니다"),
    MESSAGE_TOO_LARGE("메시지 크기가 너무 큽니다"),
    INVALID_PARAMETER("잘못된 파라미터입니다"),
    UNAUTHORIZED("인증 실패"),
    RATE_LIMIT_EXCEEDED("요청 한도 초과")
}
```

## 성능 고려사항

### Redis 최적화
- Connection Pooling 설정
- Pipeline 사용으로 네트워크 RTT 최소화
- 적절한 TTL 설정으로 메모리 사용량 관리

### 큐 처리량 최적화
- Batch 작업 지원
- Long Polling으로 불필요한 호출 감소
- 큐별 독립적인 처리를 위한 파티셔닝

### 모니터링 지표
- 큐별 메시지 수 (available, in-flight, delayed)
- 처리 지연 시간 (age of oldest message)
- 에러율 및 DLQ 메시지 수
- API 응답 시간

## 배포 및 운영

### Docker Compose 구성
```yaml
version: '3.8'
services:
  app:
    build: .
    ports:
      - "8080:8080"
    environment:
      - SPRING_REDIS_HOST=redis
    depends_on:
      - redis
      
  redis:
    image: redis:7-alpine
    ports:
      - "6379:6379"
    volumes:
      - redis_data:/data
      
volumes:
  redis_data:
```

### 환경 설정
```yaml
# application.yml
spring:
  redis:
    host: localhost
    port: 6379
    timeout: 2000ms
    lettuce:
      pool:
        max-active: 10
        max-idle: 5
        
app:
  queue:
    default-visibility-timeout: 30
    max-message-size: 262144  # 256KB
    max-messages-per-batch: 10
    scheduler-interval: 30000  # 30초
```

## 향후 확장 계획

### Phase 2
- [ ] 메시지 필터링 (Message Filtering)
- [ ] 큐 암호화 지원
- [ ] CloudWatch 스타일 메트릭 대시보드
- [ ] WebSocket을 통한 실시간 알림

### Phase 3  
- [ ] Multi-AZ 지원 (Redis Cluster)
- [ ] 메시지 중복 제거 (Deduplication)
- [ ] 트랜잭션 지원
- [ ] GraphQL API

이 스펙은 사이드 프로젝트로 시작하여 점진적으로 기능을 확장할 수 있도록 설계되었습니다. 핵심 SQS 기능을 모두 포함하면서도 구현 복잡도를 적절히 조절했습니다.



